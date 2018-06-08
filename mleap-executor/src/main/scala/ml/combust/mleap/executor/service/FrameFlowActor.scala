package ml.combust.mleap.executor.service

import akka.pattern.pipe
import akka.actor.{Actor, Props, Status}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import ml.combust.mleap.executor.service.LocalTransformServiceActor.{Messages => TMessages}
import ml.combust.mleap.executor.{ExecuteTransform, TransformFrameRequest}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}

import scala.concurrent.{Future, Promise}
import scala.util.Try

object FrameFlowActor {
  def props(transformer: Transformer,
            flow: TMessages.FrameFlow)
           (implicit materializer: Materializer): Props = {
    Props(new FrameFlowActor(transformer, flow))
  }
  
  object Messages {
    case class TransformFrame(request: TransformFrameRequest, tag: Any)
    case object GetDone
    case object StreamClosed
  }
}

class FrameFlowActor(transformer: Transformer,
                     flow: TMessages.FrameFlow)
                    (implicit materializer: Materializer) extends Actor {
  import FrameFlowActor.Messages
  import context.dispatcher

  private val queue = {
    val source = Source.queue[(Messages.TransformFrame, Promise[(Try[DefaultLeapFrame], Any)])](flow.config.bufferSize, OverflowStrategy.backpressure)
    val transform = Flow[(Messages.TransformFrame, Promise[(Try[DefaultLeapFrame], Any)])].mapAsyncUnordered(flow.config.parallelism) {
      case (tFrame, promise) =>
        ExecuteTransform(transformer, tFrame.request).map {
          frame => (frame, tFrame.tag, promise)
        }
    }.to(Sink.foreach {
      case (frame, tag, promise) => promise.success((frame, tag))
    })

    source.toMat(transform)(Keep.left).run()
  }

  queue.watchCompletion().
    map(_ => Messages.StreamClosed).
    pipeTo(self)
  private var queueF = Future(queue)


  override def postStop(): Unit = {
    queue.complete()
  }

  override def receive: Receive = {
    case r: Messages.TransformFrame => transformFrame(r)
    case Messages.GetDone => getDone()
    case Messages.StreamClosed => context.stop(self)

    case Status.Failure(err) => throw err
  }

  def transformFrame(frame: Messages.TransformFrame): Unit = {
    val promise: Promise[(Try[DefaultLeapFrame], Any)] = Promise()
    val s = sender

    queueF = queueF.flatMap {
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
    }
  }

  def getDone(): Unit = {
    sender ! queue.watchCompletion()
  }
}
