package ml.combust.mleap.executor.service

import akka.actor.{Actor, Props, ReceiveTimeout, Status}
import akka.pattern.pipe
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{DelayOverflowStrategy, Materializer, OverflowStrategy, QueueOfferResult}
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.error.ExecutorException
import ml.combust.mleap.runtime.frame.{Row, RowTransformer, Transformer}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object RowStreamActor {

  def props(transformer: Transformer,
            request: CreateRowStreamRequest,
            config: ExecutorStreamConfig)
           (implicit materializer: Materializer): Props = {
    Props(new RowStreamActor(transformer, request, config))
  }

  object Messages {
    case object Initialize
    case class TransformRow(row: StreamTransformRowRequest, promise: Promise[Try[Option[Row]]])
    case object StreamClosed
  }
}

class RowStreamActor(transformer: Transformer,
                     request: CreateRowStreamRequest,
                     config: ExecutorStreamConfig)
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
  private var queue: Option[SourceQueueWithComplete[Messages.TransformRow]] = None
  private var queueF: Option[Future[SourceQueueWithComplete[Messages.TransformRow]]] = None


  override def postStop(): Unit = {
    for (q <- queue) { q.complete() }
  }

  override def receive: Receive = {
    case r: Messages.TransformRow => transformRow(r)
    case Messages.Initialize => initialize()
    case Messages.StreamClosed => context.stop(self)

    case r: GetRowStreamRequest => getRowStream(r)
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
            request.streamConfig.getOrElse(StreamConfig()),
            request.spec,
            rt.outputSchema))

          queue = Some {
            var source = Source.queue[Messages.TransformRow](rowStream.get.streamConfig.bufferSize.getOrElse(config.defaultBufferSize), OverflowStrategy.backpressure)

            source = rowStream.get.streamConfig.idleTimeout.orElse(config.defaultIdleTimeout).map {
              timeout =>
                source.idleTimeout(timeout)
            }.getOrElse(source)

            source = rowStream.get.streamConfig.throttle.orElse(config.defaultThrottle).map {
              throttle =>
                source.throttle(throttle.elements, throttle.duration, throttle.maxBurst, throttle.mode)
            }.getOrElse(source)

            source = rowStream.get.streamConfig.transformDelay.orElse(config.defaultTransformDelay).map {
              delay =>
                source.delay(delay, DelayOverflowStrategy.backpressure)
            }.getOrElse(source)

            val transform = Flow[Messages.TransformRow].mapAsyncUnordered(rowStream.get.streamConfig.parallelism.getOrElse(config.defaultParallelism).get) {
              case Messages.TransformRow(tRow, promise) =>
                Future {
                  val row = tRow.row.map(rt.transformOption)

                  (row, promise)
                }
            }.to(Sink.foreach {
              case (row, promise) => promise.success(row)
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
    queueF = Some(queueF.get.flatMap {
      q =>
        q.offer(row).map {
          case QueueOfferResult.Enqueued => q
          case QueueOfferResult.Failure(err) =>
            row.promise.failure(err)
            q
          case QueueOfferResult.Dropped =>
            row.promise.failure(new ExecutorException("item dropped"))
            q
          case QueueOfferResult.QueueClosed =>
            row.promise.failure(new ExecutorException("queue closed"))
            q
        }
    })
  }

  def getRowStream(request: GetRowStreamRequest): Unit = {
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
