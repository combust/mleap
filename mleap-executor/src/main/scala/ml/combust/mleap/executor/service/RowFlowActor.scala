package ml.combust.mleap.executor.service

import akka.actor.{Actor, Props, Status}
import akka.pattern.pipe
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import ml.combust.mleap.executor.service.LocalTransformServiceActor.{Messages => TMessages}
import ml.combust.mleap.runtime.frame.{Row, RowTransformer}

import scala.concurrent.{Future, Promise}
import scala.util.Try

object RowFlowActor {

  def props(rowTransformer: RowTransformer,
            flow: TMessages.RowFlow)
           (implicit materializer: Materializer): Props = {
    Props(new RowFlowActor(rowTransformer, flow))
  }

  object Messages {
    case object GetRowTransformer
    case class TransformRow(row: Try[Row], tag: Any)
    case object StreamClosed
  }
}

class RowFlowActor(rowTransformer: RowTransformer,
                   flow: TMessages.RowFlow)
                  (implicit materializer: Materializer) extends Actor {
  import RowFlowActor.Messages
  import context.dispatcher

  private val queue = {
    val source = Source.queue[(Messages.TransformRow, Promise[(Try[Option[Row]], Any)])](flow.config.bufferSize, OverflowStrategy.backpressure)
    val transform = Flow[(Messages.TransformRow, Promise[(Try[Option[Row]], Any)])].mapAsyncUnordered(flow.config.parallelism) {
      case (tRow, promise) =>
        Future {
          val row = tRow.row.map {
            row =>
              rowTransformer.transformOption(row)
          }

          (row, tRow.tag, promise)
        }
    }.to(Sink.foreach {
      case (row, tag, promise) => promise.success((row, tag))
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
    case r: Messages.TransformRow => transformRow(r)
    case Messages.GetRowTransformer => getRowTransformer()
    case Messages.StreamClosed => context.stop(self)

    case Status.Failure(err) => throw err
  }

  def transformRow(row: Messages.TransformRow): Unit = {
    val promise: Promise[(Try[Option[Row]], Any)] = Promise()
    val s = sender

    queueF = queueF.flatMap {
      q =>
        q.offer((row, promise)).map {
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

  def getRowTransformer(): Unit = {
    sender ! (rowTransformer, queue.watchCompletion())
  }
}
