package ml.combust.mleap.grpc.server

import java.net.URI
import java.util.concurrent.TimeUnit

import io.grpc.stub.StreamObserver
import ml.combust.mleap.executor.{MleapExecutor, StreamRowSpec}
import ml.combust.mleap.pb._
import ml.combust.mleap.pb.MleapGrpc.Mleap
import ml.combust.mleap.runtime.serialization.{FrameReader, FrameWriter, RowReader, RowWriter}
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.google.protobuf.ByteString
import ml.combust.mleap.grpc.stream.GrpcAkkaStreams
import ml.combust.mleap.grpc.TypeConverters._
import akka.NotUsed
import akka.stream.Materializer
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap
import ml.combust.mleap.runtime.frame.Row

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class GrpcServer(executor: MleapExecutor)
                (implicit ec: ExecutionContext,
                 materializer: Materializer) extends Mleap {
  private val DEFAULT_TIMEOUT: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)

  def getTimeout(ms: Int): FiniteDuration = if (ms == 0) {
    DEFAULT_TIMEOUT
  } else { FiniteDuration(ms, TimeUnit.MILLISECONDS) }

  override def getBundleMeta(request: GetBundleMetaRequest): Future[BundleMeta] = {
    executor.getBundleMeta(URI.create(request.uri))(1.minute).map {
      meta =>
        BundleMeta(bundle = Some(meta.info.asBundle),
          inputSchema = Some(meta.inputSchema),
          outputSchema = Some(meta.outputSchema))
    }
  }

  override def transformFrame(request: TransformFrameRequest): Future[TransformFrameResponse] = {
    implicit val timeout: FiniteDuration = getTimeout(request.timeout)
    val frame = FrameReader(request.format).fromBytes(request.frame.toByteArray)

    executor.transform(
      URI.create(request.uri),
      mleap.executor.TransformFrameRequest(frame, request.options)
    )(getTimeout(request.timeout)).map {
      frame =>
        Future.fromTry(FrameWriter(frame, request.format).toBytes().map {
          bytes =>
            TransformFrameResponse(tag = request.tag,
              frame = ByteString.copyFrom(bytes),
              status = TransformStatus.STATUS_OK)
        })
    }.flatMap(identity).recover {
      case error => TransformFrameResponse(
        status = TransformStatus.STATUS_ERROR,
        error = error.getMessage,
        backtrace = error.getStackTrace.mkString("\n"))
    }
  }

  override def transformFrameStream(responseObserver: StreamObserver[TransformFrameResponse]): StreamObserver[TransformFrameRequest] = ???

  override def transformRowStream(responseObserver: StreamObserver[TransformRowResponse]): StreamObserver[TransformRowRequest] = {
    val firstObserver = new StreamObserver[TransformRowRequest] {
      private var observer: Option[StreamObserver[TransformRowRequest]] = None

      override def onError(t: Throwable): Unit = observer.foreach(_.onError(t))
      override def onCompleted(): Unit = observer.foreach(_.onCompleted())
      override def onNext(value: TransformRowRequest): Unit = {
        observer.getOrElse {
          val options: mleap.executor.TransformOptions = value.options
          val schema: StructType = value.schema.get
          val spec: StreamRowSpec = StreamRowSpec(schema, options)
          val rowReader = RowReader(schema, value.format)
          val rowWriter = RowWriter(schema, value.format)

          val rowFlow = executor.rowFlow[ByteString](URI.create(value.uri), spec)(getTimeout(value.timeout))
          val source = GrpcAkkaStreams.source[TransformRowRequest].map {
            request => (rowReader.fromBytes(request.row.toByteArray), request.tag)
          }
          val sink: Sink[(Try[Option[Row]], ByteString), NotUsed] = GrpcAkkaStreams.sink(responseObserver).contramap {
            case (row: Try[Option[Row]], tag: ByteString) =>
              val serializedRow: Try[Option[ByteString]] = row.flatMap {
                _.map {
                  r => rowWriter.toBytes(r).map(ByteString.copyFrom).map(b => Some(b))
                } match {
                  case Some(r) => r
                  case None => Try(None)
                }
              }

              serializedRow match {
                case Success(r) =>
                  val brow = r.getOrElse(ByteString.EMPTY)
                  TransformRowResponse(
                    tag = tag,
                    row = brow
                  )
                case Failure(error) =>
                  TransformRowResponse(
                    tag = tag,
                    error = error.getMessage,
                    backtrace = error.getStackTrace.mkString("\n")
                  )
              }
          }
          val grpcFlow = Flow.fromSinkAndSourceMat(sink, source)(Keep.right)
          val o = rowFlow.joinMat(grpcFlow)(Keep.right).run()

          observer = Some(o)
          o
        }.onNext(value)
      }
    }

    firstObserver
  }
}
