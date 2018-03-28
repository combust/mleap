package ml.combust.mleap.executor.service

import java.net.URI
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.pattern.ask
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem}
import akka.stream.{FlowShape, javadsl}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Source, Zip}
import ml.combust.mleap.executor.{BundleMeta, StreamRowSpec, TransformFrameRequest, TransformRowRequest}
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.executor.stream.TransformStream
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer}

import scala.concurrent.duration._
import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class BundleManager(manager: TransformService,
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

  def transformRow(request: TransformRowRequest)
                  (implicit timeout: FiniteDuration): Future[Try[Option[Row]]] = {
    (actor ? request)(timeout).mapTo[Try[Option[Row]]]
  }

  def createRowTransformer(id: UUID, spec: StreamRowSpec)
                          (implicit timeout: FiniteDuration): Future[RowTransformer] = {
    (actor ? BundleActor.CreateRowTransformer(id, spec))(timeout).mapTo[RowTransformer]
  }

  def closeStream(id: UUID): Unit = actor ! BundleActor.CloseStream(id)

  def close(): Unit = actor ! BundleActor.Shutdown
}

class TransformService(loader: RepositoryBundleLoader)
                      (implicit ec: ExecutionContext,
                       arf: ActorRefFactory) {
  private val lookup: concurrent.Map[URI, BundleManager] = concurrent.TrieMap()

  def this(loader: RepositoryBundleLoader,
           system: ActorSystem) = {
    this(loader)(system.dispatcher, system)
  }

  def getBundleMeta(uri: URI)
                   (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    manager(uri).getBundleMeta()
  }

  def getBundleMeta(uri: URI, timeout: Int): Future[BundleMeta] = {
    getBundleMeta(uri)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  def transform(uri: URI, request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[DefaultLeapFrame] = {
    manager(uri).transform(request)
  }

  def transform(uri: URI,
                request: TransformFrameRequest,
                timeout: Int): Future[DefaultLeapFrame] = {
    transform(uri, request)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  def rowFlow[Tag](uri: URI,
                   spec: StreamRowSpec,
                   parallelism: Int = TransformStream.DEFAULT_PARALLELISM)
                  (implicit timeout: FiniteDuration): Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), NotUsed] = {
    Flow.fromGraph {
      GraphDSL.create() {
        implicit builder =>
          import GraphDSL.Implicits._

          val rowTransformerSource = builder.add {
            Source.lazily(() => {
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
              }
            })
          }

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

  def javaRowFlow[Tag](uri: URI,
                       spec: StreamRowSpec,
                       timeout: Int,
                       parallelism: Int = TransformStream.DEFAULT_PARALLELISM): javadsl.Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), NotUsed] = {
    rowFlow(uri, spec, parallelism)(FiniteDuration(timeout, TimeUnit.MILLISECONDS)).asJava
  }

  private def manager(uri: URI): BundleManager = {
    lookup.getOrElseUpdate(uri, new BundleManager(this, loader, uri))
  }

  def unload(uri: URI): Unit = {
    lookup.remove(uri).foreach(_.close())
  }
}
