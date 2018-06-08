package ml.combust.mleap.executor.service

import akka.pattern.pipe
import akka.actor.{Actor, Props, ReceiveTimeout, Status}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import ml.combust.mleap.executor._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

object FrameStreamActor {
  def props(transformer: Transformer,
            request: CreateFrameStreamRequest)
           (implicit materializer: Materializer): Props = {
    Props(new FrameStreamActor(transformer, request))
  }

  object Messages {
    case object Initialize
    case class TransformFrame(request: StreamTransformFrameRequest, tag: Any)
    case object StreamClosed
  }
}

class FrameStreamActor(transformer: Transformer,
                       request: CreateFrameStreamRequest)
                      (implicit materializer: Materializer) extends Actor {
  import FrameStreamActor.Messages
  import context.dispatcher

  context.setReceiveTimeout(1.minute)

  val frameStream: FrameStream = FrameStream(request.modelName,
    request.streamName,
    request.streamConfig)

  private var queue: Option[SourceQueueWithComplete[(Messages.TransformFrame, Promise[(Try[DefaultLeapFrame], Any)])]] = None
  private var queueF: Option[Future[SourceQueueWithComplete[(Messages.TransformFrame, Promise[(Try[DefaultLeapFrame], Any)])]]] = None

  override def postStop(): Unit = {
    for (q <- queue) { q.complete() }
  }

  override def receive: Receive = {
    case r: Messages.TransformFrame => transformFrame(r)
    case Messages.Initialize => initialize()
    case Messages.StreamClosed => context.stop(self)

    case r: CreateFrameFlowRequest => createFrameFlow(r)

    case ReceiveTimeout => receiveTimeout()
    case Status.Failure(err) => throw err
  }

  def initialize(): Unit = {
    if (queue.isEmpty) {
      queue = Some {
        val source = Source.queue[(Messages.TransformFrame, Promise[(Try[DefaultLeapFrame], Any)])](
          frameStream.streamConfig.bufferSize,
          OverflowStrategy.backpressure)

        val transform = Flow[(Messages.TransformFrame, Promise[(Try[DefaultLeapFrame], Any)])].
          mapAsyncUnordered(frameStream.streamConfig.parallelism) {
            case (Messages.TransformFrame(req, tag), promise) =>
              ExecuteTransform(transformer, req.frame, req.options).map {
                frame => (frame, tag, promise)
              }
          }.idleTimeout(frameStream.streamConfig.idleTimeout).
          to(Sink.foreach {
            case (frame, tag, promise) => promise.success((frame, tag))
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
    val promise: Promise[(Try[DefaultLeapFrame], Any)] = Promise()
    val s = sender

    queueF = Some(queueF.get.flatMap {
      q =>
        q.offer((frame, promise)).map {
          case QueueOfferResult.Enqueued =>
            promise.future.pipeTo(s)
            q
          case QueueOfferResult.Failure(err) =>
            promise.failure(err)
            q
          case QueueOfferResult.Dropped =>
            promise.failure(new IllegalStateException("item dropped"))
            q
          case QueueOfferResult.QueueClosed =>
            promise.failure(new IllegalStateException("queue closed"))
            q
        }
    })
  }

  def createFrameFlow(request: CreateFrameFlowRequest): Unit = {
    queue match {
      case Some(q) => sender ! (self, q.watchCompletion())
      case None => sender ! Status.Failure(new IllegalStateException(s"frame stream not initialized ${frameStream.modelName}/frame/${frameStream.streamName}"))
    }
  }

  def receiveTimeout(): Unit = {
    if (queue.isEmpty) { context.stop(self) }
  }
}
