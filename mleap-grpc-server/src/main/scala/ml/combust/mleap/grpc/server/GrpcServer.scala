package ml.combust.mleap.grpc.server

import java.net.URI
import java.util.UUID
import java.util.concurrent.TimeUnit

import io.grpc.stub.StreamObserver
import ml.combust.mleap.executor.{CreateFrameFlowRequest, MleapExecutor, RowStreamSpec, TagBytes}
import ml.combust.mleap.pb._
import ml.combust.mleap.pb.MleapGrpc.Mleap
import ml.combust.mleap.runtime.serialization.{FrameReader, FrameWriter, RowReader, RowWriter}
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source, Zip}
import com.google.protobuf.ByteString
import ml.combust.mleap.grpc.stream.GrpcAkkaStreams
import ml.combust.mleap.grpc.TypeConverters._
import akka.NotUsed
import akka.stream.{ClosedShape, Materializer}
import io.grpc
import io.grpc.Context
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap
import ml.combust.mleap.executor
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer}

import scala.language.implicitConversions
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class GrpcServer(mleapExecutor: MleapExecutor)
                (implicit ec: ExecutionContext,
                 materializer: Materializer) extends Mleap {
  import ByetStringTagBytes._
  private val DEFAULT_TIMEOUT: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)

  def getTimeout(ms: Long): FiniteDuration = if (ms == 0) {
    DEFAULT_TIMEOUT
  } else { FiniteDuration(ms, TimeUnit.MILLISECONDS) }

  implicit def deadlineToFiniteDuration(deadline: grpc.Deadline): FiniteDuration = {
    deadline.timeRemaining(TimeUnit.MILLISECONDS).millis
  }

  //  override def getBundleMeta(request: GetBundleMetaRequest): Future[BundleMeta] = {
  //    val timeout = Context.current().getDeadline.timeRemaining(TimeUnit.MILLISECONDS)
  //
  //    executor.getBundleMeta(URI.create(request.uri))(timeout.millis).map {
  //      meta =>
  //        BundleMeta(bundle = Some(meta.info.asBundle),
  //          inputSchema = Some(meta.inputSchema),
  //          outputSchema = Some(meta.outputSchema))
  //    }
  //  }
  //
  //  override def transformFrame(request: TransformFrameRequest): Future[TransformFrameResponse] = {
  //    val timeout = Context.current().getDeadline.timeRemaining(TimeUnit.MILLISECONDS)
  //    val frame = FrameReader(request.format).fromBytes(request.frame.toByteArray)
  //
  //    executor.transform(
  //      URI.create(request.uri),
  //      mleap.executor.TransformFrameRequest(frame, request.options)
  //    )(timeout.millis).map {
  //      frame =>
  //        Future.fromTry(FrameWriter(frame, request.format).toBytes().map {
  //          bytes =>
  //            TransformFrameResponse(tag = request.tag,
  //              frame = ByteString.copyFrom(bytes),
  //              status = TransformStatus.STATUS_OK)
  //        })
  //    }.flatMap(identity).recover {
  //      case error => TransformFrameResponse(
  //        status = TransformStatus.STATUS_ERROR,
  //        error = error.getMessage,
  //        backtrace = error.getStackTrace.mkString("\n"))
  //    }
  //  }
  //
  //  override def transformFrameStream(responseObserver: StreamObserver[TransformFrameResponse]): StreamObserver[TransformFrameRequest] = {
  //    val firstObserver = new StreamObserver[TransformFrameRequest] {
  //      private var observer: Option[StreamObserver[TransformFrameRequest]] = None
  //
  //      override def onError(t: Throwable): Unit = observer.foreach(_.onError(t))
  //      override def onCompleted(): Unit = observer.foreach(_.onCompleted())
  //      override def onNext(value: TransformFrameRequest): Unit = {
  //        observer match {
  //          case Some(o) => o.onNext(value)
  //          case None =>
  //            val frameReader = FrameReader(value.format)
  //
  //            val frameFlow = executor.frameFlow[ByteString](URI.create(value.uri), value.streamConfig.get)
  //            val source = GrpcAkkaStreams.source[TransformFrameRequest].map {
  //              request =>
  //                val r = mleap.executor.TransformFrameRequest(
  //                  frameReader.fromBytes(request.frame.toByteArray),
  //                  request.options.orElse(value.options)
  //                )
  //
  //                (r, request.tag)
  //            }
  //            val sink: Sink[(Try[DefaultLeapFrame], ByteString), NotUsed] = GrpcAkkaStreams.sink(responseObserver).contramap {
  //              case (tryFrame: Try[DefaultLeapFrame], tag: ByteString) =>
  //                val serializedFrame: Try[ByteString] = tryFrame.flatMap {
  //                  r => FrameWriter(r, value.format).toBytes().map(ByteString.copyFrom)
  //                }
  //
  //                serializedFrame match {
  //                  case Success(r) =>
  //                    TransformFrameResponse(
  //                      tag = tag,
  //                      frame = r
  //                    )
  //                  case Failure(error) =>
  //                    TransformFrameResponse(
  //                      tag = tag,
  //                      error = error.getMessage,
  //                      backtrace = error.getStackTrace.mkString("\n")
  //                    )
  //                }
  //            }
  //            val grpcFlow = Flow.fromSinkAndSourceMat(sink, source)(Keep.right)
  //            val o = frameFlow.joinMat(grpcFlow)(Keep.right).run()
  //
  //            observer = Some(o)
  //        }
  //      }
  //    }
  //
  //    firstObserver
  //  }
  //
  //  override def transformRowStream(responseObserver: StreamObserver[TransformRowResponse]): StreamObserver[TransformRowRequest] = {
  //    val firstObserver = new StreamObserver[TransformRowRequest] {
  //      private var observer: Option[StreamObserver[TransformRowRequest]] = None
  //
  //      override def onError(t: Throwable): Unit = observer.foreach(_.onError(t))
  //      override def onCompleted(): Unit = observer.foreach(_.onCompleted())
  //      override def onNext(value: TransformRowRequest): Unit = {
  //        observer match {
  //          case Some(so) =>
  //            so.onNext(value)
  //          case None =>
  //            val options: mleap.executor.TransformOptions = value.options
  //            val schema: StructType = value.schema.get
  //            val spec: RowStreamSpec = RowStreamSpec(schema, options)
  //            val reader = RowReader(schema, value.format)
  //
  //            val _source = GrpcAkkaStreams.source[TransformRowRequest]
  //            val _rowFlow = executor.rowFlow[ByteString](URI.create(value.uri), spec, value.streamConfig.get)
  //
  //            val graph = RunnableGraph.fromGraph(GraphDSL.create(_source, _rowFlow)(Keep.both) {
  //              implicit builder =>
  //                (source, rowFlow) =>
  //                  import GraphDSL.Implicits._
  //
  //                  val rowWriterSource = builder.add {
  //                    Flow[Future[RowTransformer]].mapAsync(1)(identity).map {
  //                      rt =>
  //                        val writer = RowWriter(rt.outputSchema, value.format)
  //                        responseObserver.onNext(TransformRowResponse(
  //                          schema = Some(rt.outputSchema)
  //                        ))
  //                        Source.repeat(writer)
  //                    }.flatMapMerge(1, (writer) => {
  //                      writer
  //                    })
  //                  }
  //
  //                  val deserializer = builder.add {
  //                    Flow[TransformRowRequest].map {
  //                      request =>
  //                        (reader.fromBytes(request.row.toByteArray), request.tag)
  //                    }
  //                  }
  //
  //                  val serializerZip = builder.add(Zip[RowWriter, (Try[Option[Row]], ByteString)])
  //                  val serializer = builder.add {
  //                    Flow[(RowWriter, (Try[Option[Row]], ByteString))].map {
  //                      case (writer, (tryRow, tag)) =>
  //                        val serializedRow: Try[Option[ByteString]] = tryRow.flatMap {
  //                          _.map {
  //                            r =>
  //                              writer.toBytes(r).map(ByteString.copyFrom).map(b => Some(b))
  //                          } match {
  //                            case Some(r) => r
  //                            case None => Try(None)
  //                          }
  //                        }
  //
  //                        serializedRow match {
  //                          case Success(r) =>
  //                            val brow = r.getOrElse(ByteString.EMPTY)
  //                            TransformRowResponse(
  //                              tag = tag,
  //                              row = brow
  //                            )
  //                          case Failure(error) =>
  //                            TransformRowResponse(
  //                              tag = tag,
  //                              error = error.getMessage,
  //                              backtrace = error.getStackTrace.mkString("\n")
  //                            )
  //                        }
  //                    }
  //                  }
  //
  //                  val sink = builder.add(GrpcAkkaStreams.sink(responseObserver))
  //
  //                  builder.materializedValue.map(_._2) ~> rowWriterSource
  //
  //                  source.out ~> deserializer
  //                  deserializer.out ~> rowFlow
  //
  //                  rowWriterSource ~> serializerZip.in0
  //                  rowFlow.out ~> serializerZip.in1
  //                  serializerZip.out ~> serializer
  //                  serializer ~> sink
  //
  //                  ClosedShape
  //            }).mapMaterializedValue(_._1)
  //
  //            val o = graph.run()
  //
  //            observer = Some(o)
  //        }
  //      }
  //    }
  //
  //    firstObserver
  //  }



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
    Future.fromTry(FrameReader(request.format).fromBytes(request.frame.toByteArray).map {
      frame =>
        executor.TransformFrameRequest(
          modelName = request.modelName,
          frame = frame,
          options = request.options.get
        )
    }).flatMap {
      r =>
        mleapExecutor.transform(r)(Context.current().getDeadline).map {
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

            val frameFlow = mleapExecutor.createFrameFlow[ByteString](CreateFrameFlowRequest(
              modelName = value.modelName,
              streamName = value.streamName,
              format = value.format,
              flowConfig = value.flowConfig.get
            ))(implicitly[TagBytes[ByteString]], value.initTimeout.millis)

            val source = GrpcAkkaStreams.source[TransformFrameRequest].map {
              request =>
                val tFrame = frameReader.fromBytes(request.frame.toByteArray)
                val r = mleap.executor.StreamTransformFrameRequest(
                  tFrame,
                  request.options.orElse(value.options))

                (r, request.tag)
            }
            val sink: Sink[(Try[DefaultLeapFrame], ByteString), NotUsed] = GrpcAkkaStreams.sink(responseObserver).contramap {
              case (tryFrame: Try[DefaultLeapFrame], tag: ByteString) =>
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

  override def transformRowStream(responseObserver: StreamObserver[TransformRowResponse]): StreamObserver[TransformRowRequest] = ???
}
