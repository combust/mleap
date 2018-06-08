package ml.combust.mleap.executor.service

import akka.pattern.ask
import akka.{Done, NotUsed}
import akka.actor.{ActorRef, ActorRefFactory}
import akka.stream.{FlowShape, KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Zip}
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.executor._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

class LocalTransformService(loader: RepositoryBundleLoader)
                           (implicit arf: ActorRefFactory) extends TransformService {
  import LocalTransformServiceActor.Messages
  import arf.dispatcher

  private val actor: ActorRef = arf.actorOf(LocalTransformServiceActor.props(loader), "transform")


  override def getBundleMeta(request: GetBundleMetaRequest)
                            (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    (actor ? request)(timeout).mapTo[BundleMeta]
  }

  override def loadModel(request: LoadModelRequest)
                        (implicit timeout: FiniteDuration): Future[Model] = {
    (actor ? request)(timeout).mapTo[Model]
  }

  override def unloadModel(request: UnloadModelRequest)
                          (implicit timeout: FiniteDuration): Future[Model] = {
    (actor ? request)(timeout).mapTo[Model]
  }

  override def createFrameStream(request: CreateFrameStreamRequest)
                                (implicit timeout: FiniteDuration): Future[FrameStream] = {
    (actor ? request)(timeout).mapTo[FrameStream]
  }

  override def createRowStream(request: CreateRowStreamRequest)
                              (implicit timeout: FiniteDuration): Future[RowStream] = {
    (actor ? request)(timeout).mapTo[RowStream]
  }

  override def transform(request: TransformFrameRequest)
                        (implicit timeout: FiniteDuration): Future[Try[DefaultLeapFrame]] = {
    (actor ? request)(timeout).mapTo[Try[DefaultLeapFrame]]
  }

  override def frameFlow[Tag](request: CreateFrameFlowRequest)
                             (implicit timeout: FiniteDuration): Flow[(StreamTransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    val actorSource = Source.lazily(
      () =>
        Source.fromFutureSource {
          val streamActor = (actor ? request)(timeout).
            mapTo[(ActorRef, Future[Done])]

          streamActor.map(_._1).map {
            actor => Source.repeat(actor).mapMaterializedValue(_ => streamActor.flatMap(_._2))
          }
        }.mapMaterializedValue(_.flatMap(identity))
    ).mapMaterializedValue(_.flatMap(identity)).viaMat(KillSwitches.single)(Keep.both)

    Flow.fromGraph(GraphDSL.create(actorSource) {
      implicit builder =>
        actorSource =>
          import GraphDSL.Implicits._

          val doneFlow = builder.add {
            Flow[(Future[Done], UniqueKillSwitch)].mapAsync(1) {
              case (f, ks) =>
                f.map(Try(_)).
                  recover {
                    case err => Failure(err)
                  }.map(done => (done, ks))
            }.to(Sink.foreach {
              case (done, ks) =>
                done match {
                  case Success(_) => ks.shutdown()
                  case Failure(err) => ks.abort(err)
                }
            })
          }

          val inFlow = builder.add {
            Flow[(StreamTransformFrameRequest, Tag)].idleTimeout(request.flowConfig.idleTimeout)
          }

          val queueFlow = builder.add {
            Flow[((StreamTransformFrameRequest, Tag), ActorRef)].mapAsync(request.flowConfig.parallelism) {
              case ((req, tag), actor) =>
                (actor ? FrameFlowActor.Messages.TransformFrame(req, tag))(request.flowConfig.transformTimeout).
                  mapTo[(Try[DefaultLeapFrame], Tag)]
            }
          }

          val zip = builder.add { Zip[(StreamTransformFrameRequest, Tag), ActorRef] }

          builder.materializedValue ~> doneFlow
          inFlow ~> zip.in0
          actorSource ~> zip.in1
          zip.out ~> queueFlow

          FlowShape(inFlow.in, queueFlow.out)
    }).mapMaterializedValue(_ => NotUsed)
  }

  override def rowFlow[Tag](request: CreateRowFlowRequest)
                           (implicit timeout: FiniteDuration): Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), Future[RowTransformer]] = {
    val actorSource = Source.lazily(
          () =>
            Source.fromFutureSource {
              val streamActor = (actor ? request)(timeout).mapTo[(ActorRef, (RowTransformer, Future[Done]))]

              streamActor.map(_._1).map {
                actor => Source.repeat(actor).mapMaterializedValue(_ => streamActor.map(_._2))
              }
            }.mapMaterializedValue(_.flatMap(identity))
        ).mapMaterializedValue(_.flatMap(identity)).viaMat(KillSwitches.single)(Keep.both)

        Flow.fromGraph(GraphDSL.create(actorSource) {
          implicit builder =>
            actorSource =>
              import GraphDSL.Implicits._

              val doneFlow = builder.add {
                Flow[(Future[(RowTransformer, Future[Done])], UniqueKillSwitch)].mapAsync(1) {
                  case (f, ks) =>
                    f.map(_._2).
                      flatMap(identity).
                      map(Try(_)).
                      recover {
                        case err => Failure(err)
                      }.map(done => (done, ks))
                }.to(Sink.foreach {
                  case (done, ks) =>
                    done match {
                      case Success(_) => ks.shutdown()
                      case Failure(err) => ks.abort(err)
                    }
                })
              }

              val inFlow = builder.add {
                Flow[(StreamTransformRowRequest, Tag)].idleTimeout(request.flowConfig.idleTimeout)
              }

              val queueFlow = builder.add {
                Flow[((StreamTransformRowRequest, Tag), ActorRef)].mapAsync(request.flowConfig.parallelism) {
                  case ((r, tag), actor) =>
                    (actor ? RowFlowActor.Messages.TransformRow(r, tag))(request.flowConfig.transformTimeout).
                      mapTo[(Try[Option[Row]], Tag)]
                }
              }

              val zip = builder.add { Zip[(StreamTransformRowRequest, Tag), ActorRef] }

              builder.materializedValue ~> doneFlow
              inFlow ~> zip.in0
              actorSource ~> zip.in1
              zip.out ~> queueFlow

              FlowShape(inFlow.in, queueFlow.out)
        }).mapMaterializedValue(_._1.map(_._1))
  }

  override def close(): Unit = actor ! Messages.Close
}
