package ml.combust.mleap.grpc.server

import java.util.concurrent.TimeUnit

import io.grpc.stub.StreamObserver
import ml.combust.mleap.executor.{CreateFrameFlowRequest, CreateRowFlowRequest, MleapExecutor}
import ml.combust.mleap.pb._
import ml.combust.mleap.pb.MleapGrpc.Mleap
import ml.combust.mleap.runtime.serialization.{FrameReader, FrameWriter, RowReader, RowWriter}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.google.protobuf.ByteString
import ml.combust.mleap.grpc.stream.GrpcAkkaStreams
import ml.combust.mleap.grpc.TypeConverters._
import akka.NotUsed
import akka.stream.Materializer
import io.grpc
import io.grpc.Context
import ml.combust.mleap
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.executor
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.types.BundleTypeConverters._

import scala.language.implicitConversions
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class GrpcServer(mleapExecutor: MleapExecutor,
                 config: GrpcServerConfig)
                (implicit ec: ExecutionContext,
                 materializer: Materializer) extends Mleap {
  private val DEFAULT_TIMEOUT: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)

  def getTimeout(ms: Long): FiniteDuration = if (ms == 0) {
    DEFAULT_TIMEOUT
  } else { FiniteDuration(ms, TimeUnit.MILLISECONDS) }

  implicit def deadlineToFiniteDuration(deadline: grpc.Deadline): FiniteDuration = {
    deadline.timeRemaining(TimeUnit.MILLISECONDS).millis
  }

  override def getBundleMeta(request: GetBundleMetaRequest): Future[BundleMeta] = {
    mleapExecutor.getBundleMeta(request)(Context.current().getDeadline).
      map(mleapToPbBundleMeta)
  }

  override def loadModel(request: LoadModelRequest): Future[Model] = {
    mleapExecutor.loadModel(request)(Context.current().getDeadline).
      map(mleapToPbModel)
  }

  override def unloadModel(request: UnloadModelRequest): Future[Model] = {
    mleapExecutor.unloadModel(request)(Context.current().getDeadline).
      map(mleapToPbModel)
  }

  override def getModel(request: GetModelRequest): Future[Model] = {
    mleapExecutor.getModel(request)(Context.current().getDeadline).
      map(mleapToPbModel)
  }

  override def createFrameStream(request: CreateFrameStreamRequest): Future[FrameStream] = {
    mleapExecutor.createFrameStream(request)(Context.current().getDeadline).
      map(mleapToPbFrameStream)
  }

  override def getFrameStream(request: GetFrameStreamRequest): Future[FrameStream] = {
    mleapExecutor.getFrameStream(request)(Context.current().getDeadline).
      map(mleapToPbFrameStream)
  }

  override def createRowStream(request: CreateRowStreamRequest): Future[RowStream] = {
    mleapExecutor.createRowStream(request)(Context.current().getDeadline).
      map(mleapToPbRowStream)
  }

  override def getRowStream(request: GetRowStreamRequest): Future[RowStream] = {
    mleapExecutor.getRowStream(request)(Context.current().getDeadline).
      map(mleapToPbRowStream)
  }

  override def transform(request: TransformFrameRequest): Future[TransformFrameResponse] = {
    val deadline = Context.current().getDeadline

    Future.fromTry(FrameReader(request.format).fromBytes(request.frame.toByteArray).map {
      frame =>
        executor.TransformFrameRequest(
          modelName = request.modelName,
          frame = frame,
          options = request.options.get
        )
    }).flatMap {
      r =>
        mleapExecutor.transform(r)(deadline).map {
          _.flatMap {
            frame =>
              FrameWriter(frame, request.format).toBytes()
          }
        }.map {
          case Success(bytes) =>
            TransformFrameResponse(
              frame = ByteString.copyFrom(bytes)
            )
          case Failure(err) =>
            TransformFrameResponse(
              status = TransformStatus.STATUS_ERROR,
              error = err.getMessage
            )
        }
    }
  }

  override def transformFrameStream(responseObserver: StreamObserver[TransformFrameResponse]): StreamObserver[TransformFrameRequest] = {
    val firstObserver = new StreamObserver[TransformFrameRequest] {
      private var observer: Option[StreamObserver[TransformFrameRequest]] = None

      override def onError(t: Throwable): Unit = observer.foreach(_.onError(t))
      override def onCompleted(): Unit = observer.foreach(_.onCompleted())
      override def onNext(value: TransformFrameRequest): Unit = {
        observer match {
          case Some(o) => o.onNext(value)
          case None =>
            val frameReader = FrameReader(value.format)

            val frameFlow = mleapExecutor.createFrameFlow[Long](CreateFrameFlowRequest(
              modelName = value.modelName,
              streamName = value.streamName,
              format = value.format,
              flowConfig = value.flowConfig.map(pbToMleapFlowConfig)
            ))(value.initTimeout.map(_.millis).getOrElse(config.defaultStreamInitTimeout))

            val source = GrpcAkkaStreams.source[TransformFrameRequest].map {
              request =>
                val tFrame = frameReader.fromBytes(request.frame.toByteArray)
                val r = mleap.executor.StreamTransformFrameRequest(
                  tFrame,
                  request.options.orElse(value.options))

                (r, request.tag)
            }
            val sink: Sink[(Try[DefaultLeapFrame], Long), NotUsed] = GrpcAkkaStreams.sink(responseObserver).contramap {
              case (tryFrame: Try[DefaultLeapFrame], tag: Long) =>
                val serializedFrame: Try[ByteString] = tryFrame.flatMap {
                  r => FrameWriter(r, value.format).toBytes().map(ByteString.copyFrom)
                }

                serializedFrame match {
                  case Success(r) =>
                    TransformFrameResponse(
                      tag = tag,
                      frame = r
                    )
                  case Failure(error) =>
                    TransformFrameResponse(
                      tag = tag,
                      error = error.getMessage,
                      backtrace = error.getStackTrace.mkString("\n")
                    )
                }
            }
            val grpcFlow = Flow.fromSinkAndSourceMat(sink, source)(Keep.right)
            val o = frameFlow.joinMat(grpcFlow)(Keep.right).run()

            observer = Some(o)
        }
      }
    }

    firstObserver
  }

  override def transformRowStream(responseObserver: StreamObserver[TransformRowResponse]): StreamObserver[TransformRowRequest] = {
    val firstObserver = new StreamObserver[TransformRowRequest] {
      private var observer: Option[StreamObserver[TransformRowRequest]] = None

      override def onError(t: Throwable): Unit = observer.foreach(_.onError(t))
      override def onCompleted(): Unit = observer.foreach(_.onCompleted())
      override def onNext(value: TransformRowRequest): Unit = {
        observer match {
          case Some(o) => o.onNext(value)
          case None =>
            val inputSchema: StructType = value.inputSchema.get
            val outputSchema: StructType = value.outputSchema.get
            val rowReader = RowReader(inputSchema, value.format)
            val rowWriter = RowWriter(outputSchema, value.format)

            val rowFlow = mleapExecutor.createRowFlow[Long](CreateRowFlowRequest(
              modelName = value.modelName,
              streamName = value.streamName,
              format = value.format,
              flowConfig = value.flowConfig.map(pbToMleapFlowConfig),
              inputSchema = inputSchema,
              outputSchema = outputSchema
            ))(value.initTimeout.map(_.millis).getOrElse(config.defaultStreamInitTimeout))

            val source = GrpcAkkaStreams.source[TransformRowRequest].map {
              request =>
                val tRow = rowReader.fromBytes(request.row.toByteArray)
                val r = mleap.executor.StreamTransformRowRequest(tRow)

                (r, request.tag)
            }

            val sink: Sink[(Try[Option[Row]], Long), NotUsed] = GrpcAkkaStreams.sink(responseObserver).contramap {
              case (tryRow: Try[Option[Row]], tag: Long) =>
                val serializedRow: Try[ByteString] = tryRow.flatMap {
                  r =>
                    r.map {
                      r => rowWriter.toBytes(r).map(ByteString.copyFrom)
                    }.getOrElse(Try(ByteString.EMPTY))
                }

                serializedRow match {
                  case Success(r) =>
                    TransformRowResponse(
                      tag = tag,
                      row = r
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
        }
      }
    }

    firstObserver
  }
}
