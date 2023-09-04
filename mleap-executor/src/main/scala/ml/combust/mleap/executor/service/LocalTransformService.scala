package ml.combust.mleap.executor.service

import akka.pattern.ask
import akka.{Done, NotUsed}
import akka.actor.{ActorRef, ActorRefFactory}
import akka.stream.{DelayOverflowStrategy, FlowShape, KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Zip}
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.error.{ExecutorException, TimeoutException}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

class LocalTransformService(loader: RepositoryBundleLoader,
                            config: ExecutorConfig)
                           (implicit arf: ActorRefFactory) extends TransformService {
  import LocalTransformServiceActor.Messages
  import arf.dispatcher

  private val flowConfig: ExecutorFlowConfig = config.flow
  private val actor: ActorRef = arf.actorOf(LocalTransformServiceActor.props(loader, config), "transform")

  private def wrapExceptions(err: Throwable): ExecutorException = err match {
    case err: akka.pattern.AskTimeoutException => new TimeoutException(err)
    case err: ExecutorException => err
    case _ => new ExecutorException(err)
  }

  private def wrapExceptions[T](f: Future[T]): Future[T] = {
    f.transform(identity, wrapExceptions)
  }

  override def getBundleMeta(request: GetBundleMetaRequest)
                            (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    wrapExceptions((actor ? request)(timeout).mapTo[BundleMeta])
  }

  override def getModel(request: GetModelRequest)
                       (implicit timeout: FiniteDuration): Future[Model] = {
    wrapExceptions((actor ? request)(timeout).mapTo[Model])
  }

  override def loadModel(request: LoadModelRequest)
                        (implicit timeout: FiniteDuration): Future[Model] = {
    wrapExceptions((actor ? request)(timeout).mapTo[Model])
  }

  override def unloadModel(request: UnloadModelRequest)
                          (implicit timeout: FiniteDuration): Future[Model] = {
    wrapExceptions((actor ? request)(timeout).mapTo[Model])
  }

  override def createFrameStream(request: CreateFrameStreamRequest)
                                (implicit timeout: FiniteDuration): Future[FrameStream] = {
    wrapExceptions((actor ? request)(timeout).mapTo[FrameStream])
  }

  override def createRowStream(request: CreateRowStreamRequest)
                              (implicit timeout: FiniteDuration): Future[RowStream] = {
    wrapExceptions((actor ? request)(timeout).mapTo[RowStream])
  }

  override def transform(request: TransformFrameRequest)
                        (implicit timeout: FiniteDuration): Future[Try[DefaultLeapFrame]] = {
    wrapExceptions((actor ? request)(timeout).mapTo[Try[DefaultLeapFrame]])
  }

  override def getFrameStream(request: GetFrameStreamRequest)
                             (implicit timeout: FiniteDuration): Future[FrameStream] = {
    wrapExceptions((actor ? request)(timeout).mapTo[FrameStream])
  }

  override def getRowStream(request: GetRowStreamRequest)
                           (implicit timeout: FiniteDuration): Future[RowStream] = {
    wrapExceptions((actor ? request)(timeout).mapTo[RowStream])
  }


  override def createFrameFlow[Tag](request: CreateFrameFlowRequest)
                                   (implicit timeout: FiniteDuration): Flow[(StreamTransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    val actorSource = Source.lazySource(
      () =>
        Source.futureSource {
          val streamActor = wrapExceptions((actor ? request)(timeout)).
            mapTo[(ActorRef, Future[Done])]

          streamActor.map(_._1).map {
            actor => Source.repeat(actor).mapMaterializedValue(_ => streamActor.flatMap(_._2))
          }
        }.mapMaterializedValue(_.flatten)
    ).mapMaterializedValue(_.flatten).viaMat(KillSwitches.single)(Keep.both)

    Flow.fromGraph(GraphDSL.createGraph(actorSource) {
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

          val fc = request.flowConfig.getOrElse(FlowConfig())

          val inFlow = builder.add {
            var flow = Flow[(StreamTransformFrameRequest, Tag)]

            flow = fc.idleTimeout.orElse(flowConfig.defaultIdleTimeout).map {
              timeout => flow.idleTimeout(timeout)
            }.getOrElse(flow).mapError {
              case err: java.util.concurrent.TimeoutException => new TimeoutException(err)
            }

            flow = fc.throttle.orElse(flowConfig.defaultThrottle).map {
              throttle => flow.throttle(throttle.elements, throttle.duration, throttle.maxBurst, throttle.mode)
            }.getOrElse(flow)

            flow = fc.transformDelay.orElse(flowConfig.defaultTransformDelay).map {
              delay => flow.delay(delay, DelayOverflowStrategy.backpressure)
            }.getOrElse(flow)

            flow
          }

          val queueFlow = builder.add {
            Flow[((StreamTransformFrameRequest, Tag), ActorRef)].mapAsync(fc.parallelism.getOrElse(flowConfig.defaultParallelism).get) {
              case ((req, tag), streamActor) =>
                val promise: Promise[Try[DefaultLeapFrame]] = Promise()
                streamActor ! FrameStreamActor.Messages.TransformFrame(req, promise)
                wrapExceptions(promise.future.map(frame => (frame, tag)))
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

  override def createRowFlow[Tag](request: CreateRowFlowRequest)
                                 (implicit timeout: FiniteDuration): Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    val actorSource = Source.lazySource(
      () =>
        Source.futureSource {
          val streamActor = wrapExceptions(
            (actor ? request)(timeout).mapTo[(ActorRef, RowTransformer, Future[Done])]
          )

          streamActor.map(_._1).map {
            actor => Source.repeat(actor).mapMaterializedValue(_ => streamActor.map(m => (m._2, m._3)))
          }
        }.mapMaterializedValue(_.flatten)
    ).mapMaterializedValue(_.flatten).viaMat(KillSwitches.single)(Keep.both)

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

          val fc = request.flowConfig.getOrElse(FlowConfig())

          val inFlow = builder.add {
            var flow = Flow[(StreamTransformRowRequest, Tag)]

            flow = fc.idleTimeout.orElse(flowConfig.defaultIdleTimeout).map {
              timeout => flow.idleTimeout(timeout)
            }.getOrElse(flow).mapError {
              case err: java.util.concurrent.TimeoutException => new TimeoutException(err)
            }

            flow = fc.throttle.orElse(flowConfig.defaultThrottle).map {
              throttle => flow.throttle(throttle.elements, throttle.duration, throttle.maxBurst, throttle.mode)
            }.getOrElse(flow)

            flow = fc.transformDelay.orElse(flowConfig.defaultTransformDelay).map {
              delay => flow.delay(delay, DelayOverflowStrategy.backpressure)
            }.getOrElse(flow)

            flow
          }

          val queueFlow = builder.add {
            Flow[((StreamTransformRowRequest, Tag), ActorRef)].mapAsync(fc.parallelism.getOrElse(flowConfig.defaultParallelism).get) {
              case ((r, tag), actor) =>
                val promise: Promise[Try[Option[Row]]] = Promise()
                actor ! RowStreamActor.Messages.TransformRow(r, promise)
                wrapExceptions(promise.future.map(row => (row, tag)))
            }
          }

          val zip = builder.add { Zip[(StreamTransformRowRequest, Tag), ActorRef] }

          builder.materializedValue ~> doneFlow
          inFlow ~> zip.in0
          actorSource ~> zip.in1
          zip.out ~> queueFlow

          FlowShape(inFlow.in, queueFlow.out)
    }).mapMaterializedValue(_ => NotUsed)
  }

  override def close(): Unit = actor ! Messages.Close
}
