package ml.combust.mleap.executor.service

import java.net.URI
import java.util.UUID

import akka.NotUsed
import akka.pattern.ask
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem}
import akka.stream.FlowShape
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Source, Zip}
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer, Transformer}

import scala.concurrent.duration._
import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Try}

class BundleManager(manager: LocalTransformService,
                    loader: RepositoryBundleLoader,
                    uri: URI)
                   (implicit arf: ActorRefFactory,
                    ec: ExecutionContext) {
  lazy val actor: ActorRef = arf.actorOf(BundleActor.props(manager, uri, loader.loadBundle(uri)))

  def getBundleMeta()
                   (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    (actor ? BundleActor.GetBundleMeta)(timeout).mapTo[BundleMeta]
  }

  def transform(request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[DefaultLeapFrame] = {
    (actor ? request)(timeout).mapTo[DefaultLeapFrame]
  }

  def createRowTransformer(id: UUID, spec: StreamRowSpec)
                          (implicit timeout: FiniteDuration): Future[RowTransformer] = {
    (actor ? BundleActor.CreateRowStream(id, spec))(timeout).mapTo[RowTransformer]
  }

  def createFrameStream(id: UUID)
                       (implicit timeout: FiniteDuration): Future[Transformer] = {
    (actor ? BundleActor.CreateFrameStream(id))(timeout).mapTo[Transformer]
  }

  def closeStream(id: UUID): Unit = actor ! BundleActor.CloseStream(id)

  def close(): Unit = actor ! BundleActor.Shutdown
}

class LocalTransformService(loader: RepositoryBundleLoader)
                           (implicit ec: ExecutionContext,
                            arf: ActorRefFactory) extends TransformService {
  private val lookup: concurrent.Map[URI, BundleManager] = concurrent.TrieMap()

  def this(loader: RepositoryBundleLoader,
           system: ActorSystem) = {
    this(loader)(system.dispatcher, system)
  }

  override def close(): Unit = { }

  override def getBundleMeta(uri: URI)
                            (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    manager(uri).getBundleMeta()
  }

  override def transform(uri: URI, request: TransformFrameRequest)
                        (implicit timeout: FiniteDuration): Future[DefaultLeapFrame] = {
    manager(uri).transform(request)
  }

  override def frameFlow[Tag](uri: URI)
                             (implicit timeout: FiniteDuration,
                              parallelism: Parallelism): Flow[(TransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    Flow.fromGraph {
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

          val transformerSource = builder.add {
            Source.lazily(() => {
              val id = UUID.randomUUID()
              val m = manager(uri)

              Source.fromFutureSource {
                val transformer = m.createFrameStream(id)(1.minute)
                transformer.onFailure {
                  case _ => m.closeStream(id)
                }

                transformer.map {
                  transformer => Source.repeat(transformer)
                }
              }
            })
          }

          val transformFlow = builder.add {
            Flow[(Transformer, (TransformFrameRequest, Tag))].mapAsyncUnordered(parallelism) {
              case (transformer, (request, tag)) =>
                ExecuteTransform(transformer, request).
                  map(frame => Try(frame)).
                  recover {
                    case error => Failure(error)
                  }.map(frame => (frame, tag))
            }
          }

          val frameFlow = builder.add(Flow[(TransformFrameRequest, Tag)])

          val zip = builder.add(Zip[Transformer, (TransformFrameRequest, Tag)])

          transformerSource ~> zip.in0
          frameFlow ~> zip.in1
          zip.out ~> transformFlow

          FlowShape(frameFlow.in, transformFlow.out)
      }
    }
  }

  override def rowFlow[Tag](uri: URI,
                            spec: StreamRowSpec)
                           (implicit timeout: FiniteDuration,
                            parallelism: Parallelism): Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), Future[RowTransformer]] = {
    val _rowTransformerSource = Source.lazily(() => {
      val id = UUID.randomUUID()
      val m = manager(uri)

      // this timeout should probably be configurable
      val rowTransformer = m.createRowTransformer(id, spec)(1.minute)
      rowTransformer.onFailure {
        case _ => m.closeStream(id)
      }

      Source.fromFutureSource {
        rowTransformer.map {
          rt =>
            Source.repeat(rt).watchTermination()(Keep.right).mapMaterializedValue {
              done =>
                done.andThen { case _ => m.closeStream(id) }
            }
        }
      }.mapMaterializedValue(_ => rowTransformer)
    }).mapMaterializedValue(_.flatMap(identity))

    Flow.fromGraph {
      GraphDSL.create(_rowTransformerSource) {
        implicit builder =>
          rowTransformerSource =>
            import GraphDSL.Implicits._

            val transformFlow = builder.add {
              Flow[(RowTransformer, (Try[Row], Tag))].mapAsyncUnordered(parallelism) {
                case (rowTransformer, (row, tag)) =>
                  Future {
                    val result = row.flatMap {
                      r => Try(rowTransformer.transformOption(r))
                    }

                    (result, tag)
                  }
              }
            }

            val flow = builder.add { Flow[(Try[Row], Tag)] }

            val transformZip = builder.add { Zip[RowTransformer, (Try[Row], Tag)] }

            rowTransformerSource ~> transformZip.in0
            flow ~> transformZip.in1
            transformZip.out ~> transformFlow

            FlowShape(flow.in, transformFlow.out)
      }
    }
  }

  override def unload(uri: URI): Unit = {
    lookup.remove(uri).foreach(_.close())
  }

  private def manager(uri: URI): BundleManager = {
    lookup.getOrElseUpdate(uri, new BundleManager(this, loader, uri))
  }
}
