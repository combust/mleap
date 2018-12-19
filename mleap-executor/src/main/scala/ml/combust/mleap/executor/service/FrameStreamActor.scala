package ml.combust.mleap.executor.service

import akka.pattern.pipe
import akka.actor.{Actor, Props, ReceiveTimeout, Status}
import akka.stream.{DelayOverflowStrategy, Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.error.ExecutorException
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

object FrameStreamActor {
  def props(transformer: Transformer,
            request: CreateFrameStreamRequest,
            config: ExecutorStreamConfig)
           (implicit materializer: Materializer): Props = {
    Props(new FrameStreamActor(transformer, request, config))
  }

  object Messages {
    case object Initialize
    case class TransformFrame(request: StreamTransformFrameRequest, promise: Promise[Try[DefaultLeapFrame]])
    case object StreamClosed
  }
}

class FrameStreamActor(transformer: Transformer,
                       request: CreateFrameStreamRequest,
                       config: ExecutorStreamConfig)
                      (implicit materializer: Materializer) extends Actor {
  import FrameStreamActor.Messages
  import context.dispatcher

  context.setReceiveTimeout(1.minute)

  val frameStream: FrameStream = FrameStream(request.modelName,
    request.streamName,
    request.streamConfig.getOrElse(StreamConfig()))

  private var queue: Option[SourceQueueWithComplete[Messages.TransformFrame]] = None
  private var queueF: Option[Future[SourceQueueWithComplete[Messages.TransformFrame]]] = None

  override def postStop(): Unit = {
    for (q <- queue) { q.complete() }
  }

  override def receive: Receive = {
    case r: Messages.TransformFrame => transformFrame(r)
    case Messages.Initialize => initialize()
    case Messages.StreamClosed => context.stop(self)

    case r: GetFrameStreamRequest => getFrameStream(r)
    case r: CreateFrameFlowRequest => createFrameFlow(r)

    case ReceiveTimeout => receiveTimeout()
    case Status.Failure(err) => throw err
  }

  def initialize(): Unit = {
    if (queue.isEmpty) {
      queue = Some {
        var source = Source.queue[Messages.TransformFrame](
          frameStream.streamConfig.bufferSize.getOrElse(config.defaultBufferSize),
          OverflowStrategy.backpressure)

        source = frameStream.streamConfig.idleTimeout.orElse(config.defaultIdleTimeout).map {
          timeout => source.idleTimeout(timeout)
        }.getOrElse(source)

        source = frameStream.streamConfig.throttle.orElse(config.defaultThrottle).map {
          throttle => source.throttle(throttle.elements, throttle.duration, throttle.maxBurst, throttle.mode)
        }.getOrElse(source)

        source = frameStream.streamConfig.transformDelay.orElse(config.defaultTransformDelay).map {
          delay =>
            source.delay(delay, DelayOverflowStrategy.backpressure)
        }.getOrElse(source)

        val transform = Flow[Messages.TransformFrame].
          mapAsyncUnordered(frameStream.streamConfig.parallelism.getOrElse(config.defaultParallelism).get) {
            case Messages.TransformFrame(req, promise) =>
              ExecuteTransform(transformer, req.frame, req.options).map {
                frame => (frame, promise)
              }
          }.to(Sink.foreach {
            case (frame, promise) => promise.success(frame)
          })

        source.toMat(transform)(Keep.left).run()
      }

      queue.get.watchCompletion().
        map(_ => Messages.StreamClosed).
        pipeTo(self)

      queueF = Some(Future(queue.get))
      context.setReceiveTimeout(Duration.Inf)
    }

    sender ! frameStream
  }

  def transformFrame(frame: Messages.TransformFrame): Unit = {
    queueF = Some(queueF.get.flatMap {
      q =>
        q.offer(frame).map {
          case QueueOfferResult.Enqueued => q
          case QueueOfferResult.Failure(err) =>
            frame.promise.failure(err)
            q
          case QueueOfferResult.Dropped =>
            frame.promise.failure(new ExecutorException("item dropped"))
            q
          case QueueOfferResult.QueueClosed =>
            frame.promise.failure(new ExecutorException("queue closed"))
            q
        }
    })
  }

  def getFrameStream(request: GetFrameStreamRequest): Unit = {
    sender ! frameStream
  }

  def createFrameFlow(request: CreateFrameFlowRequest): Unit = {
    queue match {
      case Some(q) => sender ! (self, q.watchCompletion())
      case None => sender ! Status.Failure(new ExecutorException(s"frame stream not initialized ${frameStream.modelName}/frame/${frameStream.streamName}"))
    }
  }

  def receiveTimeout(): Unit = {
    if (queue.isEmpty) { context.stop(self) }
  }
}
