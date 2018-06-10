package ml.combust.mleap.executor.service

import akka.actor.{Actor, Props, ReceiveTimeout, Status}
import akka.pattern.pipe
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.error.ExecutorException
import ml.combust.mleap.runtime.frame.{Row, RowTransformer, Transformer}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object RowStreamActor {

  def props(transformer: Transformer,
            request: CreateRowStreamRequest)
           (implicit materializer: Materializer): Props = {
    Props(new RowStreamActor(transformer, request))
  }

  object Messages {
    case object Initialize
    case class TransformRow(row: StreamTransformRowRequest, tag: Any)
    case object StreamClosed
  }
}

class RowStreamActor(transformer: Transformer,
                     request: CreateRowStreamRequest)
                    (implicit materializer: Materializer) extends Actor {
  import RowStreamActor.Messages
  import context.dispatcher

  context.setReceiveTimeout(1.minute)

  val rowTransformer: Try[RowTransformer] = transformer.transform(RowTransformer(request.spec.schema)).flatMap {
    rt =>
      request.spec.options.select.map {
        s =>
          request.spec.options.selectMode match {
            case SelectMode.Strict => rt.select(s: _*)
            case SelectMode.Relaxed => Try(rt.relaxedSelect(s: _*))
          }
      }.getOrElse(Try(rt))
  }

  private var rowStream: Option[RowStream] = None
  private var queue: Option[SourceQueueWithComplete[(Messages.TransformRow, Promise[(Try[Option[Row]], Any)])]] = None
  private var queueF: Option[Future[SourceQueueWithComplete[(Messages.TransformRow, Promise[(Try[Option[Row]], Any)])]]] = None


  override def postStop(): Unit = {
    for (q <- queue) { q.complete() }
  }

  override def receive: Receive = {
    case r: Messages.TransformRow => transformRow(r)
    case Messages.Initialize => initialize()
    case Messages.StreamClosed => context.stop(self)

    case r: GetRowStreamRequest => getRowStream()
    case r: CreateRowFlowRequest => createRowFlow(r)

    case ReceiveTimeout => receiveTimeout()
    case Status.Failure(err) => throw err
  }

  def initialize(): Unit = {
    rowTransformer match {
      case Success(rt) =>
        if (queue.isEmpty) {
          rowStream = Some(RowStream(request.modelName,
            request.streamName,
            request.streamConfig,
            request.spec,
            rt.outputSchema))

          queue = Some {
            var source = Source.queue[(Messages.TransformRow, Promise[(Try[Option[Row]], Any)])](rowStream.get.streamConfig.bufferSize, OverflowStrategy.backpressure)
            source = rowStream.get.streamConfig.throttle.map {
              throttle =>
                source.throttle(throttle.elements, throttle.duration, throttle.maxBurst, throttle.mode)
            }.getOrElse(source)

            val transform = Flow[(Messages.TransformRow, Promise[(Try[Option[Row]], Any)])].mapAsyncUnordered(rowStream.get.streamConfig.parallelism) {
              case (Messages.TransformRow(tRow, tag), promise) =>
                Future {
                  val row = tRow.row.map(rt.transformOption)

                  (row, tag, promise)
                }
            }.idleTimeout(rowStream.get.streamConfig.idleTimeout).to(Sink.foreach {
              case (row, tag, promise) => promise.success((row, tag))
            })

            source.toMat(transform)(Keep.left).run()
          }

          queue.get.watchCompletion().
            map(_ => Messages.StreamClosed).
            pipeTo(self)

          queueF = Some(Future(queue.get))
        }

        sender ! rowStream.get
      case Failure(err) => sender ! Status.Failure(err)
    }
  }

  def transformRow(row: Messages.TransformRow): Unit = {
    val promise: Promise[(Try[Option[Row]], Any)] = Promise()
    val s = sender

    queueF = Some(queueF.get.flatMap {
      q =>
        q.offer((row, promise)).map {
          case QueueOfferResult.Enqueued =>
            promise.future.pipeTo(s)
            q
          case QueueOfferResult.Failure(err) =>
            promise.failure(err)
            q
          case QueueOfferResult.Dropped =>
            promise.failure(new ExecutorException("item dropped"))
            q
          case QueueOfferResult.QueueClosed =>
            promise.failure(new ExecutorException("queue closed"))
            q
        }
    })
  }

  def getRowStream(): Unit = {
    rowStream match {
      case Some(rs) => sender ! rs
      case None => sender ! Status.Failure(new ExecutorException("stream not yet loaded"))
    }
  }

  def createRowFlow(request: CreateRowFlowRequest): Unit = {
    queue match {
      case Some(q) =>
        rowTransformer match {
          case Success(rt) => sender ! (self, rt, q.watchCompletion())
          case Failure(err) => sender ! Status.Failure(err)
        }
      case None => sender ! Status.Failure(new ExecutorException(s"row stream not initialized ${rowStream.get.modelName}/row/${rowStream.get.streamName}"))
    }
  }

  def receiveTimeout(): Unit = {
    if (queue.isEmpty) { context.stop(self) }
  }
}
