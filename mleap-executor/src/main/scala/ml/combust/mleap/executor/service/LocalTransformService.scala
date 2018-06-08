package ml.combust.mleap.executor.service

import java.net.URI

import akka.pattern.ask
import akka.{Done, NotUsed}
import akka.actor.{ActorRef, ActorRefFactory}
import akka.stream.{FlowShape, KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Zip}
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.executor.{BundleMeta, StreamConfig, StreamRowSpec, TransformFrameRequest}
import ml.combust.mleap.executor.service.LocalTransformServiceActor.Messages.{FrameFlow, RowFlow}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

class LocalTransformService(loader: RepositoryBundleLoader)
                           (implicit arf: ActorRefFactory) extends TransformService {
  import LocalTransformServiceActor.Messages
  import arf.dispatcher

  private val actor: ActorRef = arf.actorOf(LocalTransformServiceActor.props(loader), "transform")

  override def getBundleMeta(uri: URI)
                            (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    (actor ? Messages.GetBundleMeta(uri))(timeout).mapTo[BundleMeta]
  }

  override def transform(uri: URI,
                         request: TransformFrameRequest)
                        (implicit timeout: FiniteDuration): Future[DefaultLeapFrame] = {
    (actor ? Messages.Transform(uri, request))(timeout).mapTo[DefaultLeapFrame]
  }

  override def frameFlow[Tag](uri: URI,
                              config: StreamConfig): Flow[(TransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    val actorSource = Source.lazily(
      () =>
        Source.fromFutureSource {
          val streamActor = (actor ? FrameFlow(uri, config))(config.initTimeout).
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
            Flow[(TransformFrameRequest, Tag)]
          }

          val queueFlow = builder.add {
            Flow[((TransformFrameRequest, Tag), ActorRef)].mapAsync(config.parallelism) {
              case ((req, tag), actor) =>
                (actor ? FrameFlowActor.Messages.TransformFrame(req, tag))(config.transformTimeout).
                  mapTo[(Try[DefaultLeapFrame], Tag)]
            }
          }

          val zip = builder.add { Zip[(TransformFrameRequest, Tag), ActorRef] }

          builder.materializedValue ~> doneFlow
          inFlow ~> zip.in0
          actorSource ~> zip.in1
          zip.out ~> queueFlow

          FlowShape(inFlow.in, queueFlow.out)
    }).mapMaterializedValue(_ => NotUsed)
  }

  override def rowFlow[Tag](uri: URI,
                            spec: StreamRowSpec,
                            config: StreamConfig): Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), Future[RowTransformer]] = {
    val actorSource = Source.lazily(
      () =>
        Source.fromFutureSource {
          val streamActor = (actor ? RowFlow(uri, spec, config))(config.initTimeout).mapTo[(ActorRef, (RowTransformer, Future[Done]))]

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
            Flow[(Try[Row], Tag)]
          }

          val queueFlow = builder.add {
            Flow[((Try[Row], Tag), ActorRef)].mapAsync(config.parallelism) {
              case ((tryRow, tag), actor) =>
                (actor ? RowFlowActor.Messages.TransformRow(tryRow, tag))(config.transformTimeout).
                  mapTo[(Try[Option[Row]], Tag)]
            }
          }

          val zip = builder.add { Zip[(Try[Row], Tag), ActorRef] }

          builder.materializedValue ~> doneFlow
          inFlow ~> zip.in0
          actorSource ~> zip.in1
          zip.out ~> queueFlow

          FlowShape(inFlow.in, queueFlow.out)
    }).mapMaterializedValue(_._1.map(_._1))
  }

  override def unload(uri: URI): Unit = actor ! Messages.Unload(uri)

  override def close(): Unit = actor ! Messages.Close
}
