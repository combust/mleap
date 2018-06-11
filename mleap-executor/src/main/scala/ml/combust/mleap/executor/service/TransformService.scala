package ml.combust.mleap.executor.service

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult, javadsl}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.error.ExecutorException
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class TransformRowClient(private var queue: Future[SourceQueueWithComplete[(StreamTransformRowRequest, Promise[Try[Option[Row]]])]])
                        (implicit ec: ExecutionContext) {
  def transform(request: StreamTransformRowRequest): Future[Try[Option[Row]]] = {
    val promise: Promise[Try[Option[Row]]] = Promise()

    synchronized {
      queue = queue.flatMap {
        q =>
          q.offer((request, promise)).map {
            case QueueOfferResult.Enqueued => q
            case QueueOfferResult.QueueClosed =>
              promise.failure(new ExecutorException("queue closed"))
              q
            case QueueOfferResult.Dropped =>
              promise.failure(new ExecutorException("element dropped"))
              q
            case QueueOfferResult.Failure(err) =>
              promise.failure(new ExecutorException(err))
              q
            case _ =>
              promise.failure(new ExecutorException("unknown transform error"))
              q
          }
      }

      promise.future
    }
  }

  def transform(row: Row): Future[Try[Option[Row]]] = transform(StreamTransformRowRequest(Try(row)))
}

trait TransformService {
  def close(): Unit

  def getBundleMeta(request: GetBundleMetaRequest)
                   (implicit timeout: FiniteDuration): Future[BundleMeta]

  def getBundleMeta(request: GetBundleMetaRequest, timeout: Int): Future[BundleMeta] = {
    getBundleMeta(request)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  def getModel(request: GetModelRequest)
              (implicit timeout: FiniteDuration): Future[Model]

  def loadModel(request: LoadModelRequest)
               (implicit timeout: FiniteDuration): Future[Model]

  def unloadModel(request: UnloadModelRequest)
                 (implicit timeout: FiniteDuration): Future[Model]

  def createFrameStream(request: CreateFrameStreamRequest)
                       (implicit timeout: FiniteDuration): Future[FrameStream]

  def getFrameStream(request: GetFrameStreamRequest)
                    (implicit timeout: FiniteDuration): Future[FrameStream]

  def createRowStream(request: CreateRowStreamRequest)
                     (implicit timeout: FiniteDuration): Future[RowStream]

  def getRowStream(request: GetRowStreamRequest)
                  (implicit timeout: FiniteDuration): Future[RowStream]

  def transform(request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[Try[DefaultLeapFrame]]

  def transform(request: TransformFrameRequest,
                timeout: Int): Future[Try[DefaultLeapFrame]] = {
    transform(request)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  def createFrameFlow[Tag](request: CreateFrameFlowRequest)
                          (implicit timeout: FiniteDuration): Flow[(StreamTransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed]

  def createRowFlow[Tag](request: CreateRowFlowRequest)
                        (implicit timeout: FiniteDuration): Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed]

  def javaRowFlow[Tag](request: CreateRowFlowRequest,
                       timeout: Long): javadsl.Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    createRowFlow(request)(timeout.millis).asJava
  }

  def createTransformRowClient(request: CreateRowFlowRequest,
                               bufferSize: Int)
                              (implicit timeout: FiniteDuration,
                               materializer: Materializer,
                               ec: ExecutionContext): TransformRowClient = {
    val queue = Source.queue[(StreamTransformRowRequest, Promise[Try[Option[Row]]])](bufferSize, OverflowStrategy.backpressure)
    val q = queue.viaMat(createRowFlow[Promise[Try[Option[Row]]]](request))(Keep.left).
      toMat(Sink.foreach {
        case (response, promise) => promise.success(response)
      })(Keep.left).run()

    new TransformRowClient(Future.successful(q))
  }
}
