package ml.combust.mleap.grpc

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source, Zip}
import ml.combust.mleap.executor._
import ml.combust.mleap.pb.{TransformFrameResponse, TransformRowResponse, TransformStatus}
import ml.combust.mleap.pb.MleapGrpc.MleapStub
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import TypeConverters._
import akka.stream.FlowShape
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import ml.combust.mleap.executor.service.TransformService
import ml.combust.mleap.grpc.stream.GrpcAkkaStreams
import ml.combust.mleap.{executor, pb}
import ml.combust.mleap.runtime.serialization._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

class GrpcClient(stub: MleapStub)
                (implicit ec: ExecutionContext) extends TransformService {
  private val format: String = BuiltinFormats.binary
  private val reader: FrameReader = FrameReader(format)

  override def getBundleMeta(request: executor.GetBundleMetaRequest)
                            (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).
      getBundleMeta(request).
      map(pbToMleapBundleMeta)
  }

  override def loadModel(request: LoadModelRequest)
                        (implicit timeout: FiniteDuration): Future[Model] = {
    stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).
      loadModel(request).
      map(pbToMleapModel)
  }

  override def unloadModel(request: UnloadModelRequest)
                          (implicit timeout: FiniteDuration): Future[Model] = {
    stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).
      unloadModel(request).
      map(pbToMleapModel)
  }

  override def createFrameStream(request: CreateFrameStreamRequest)
                                (implicit timeout: FiniteDuration): Future[FrameStream] = {
    stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).
      createFrameStream(request).
      map(pbToMleapFrameStream)
  }

  override def createRowStream(request: CreateRowStreamRequest)
                              (implicit timeout: FiniteDuration): Future[RowStream] = {
    stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).
      createRowStream(request).
      map(pbToMleapRowStream)
  }

  override def transform(request: TransformFrameRequest)
                        (implicit timeout: FiniteDuration): Future[Try[DefaultLeapFrame]] = {
    Future.fromTry(FrameWriter(request.frame, format).toBytes().map {
      bytes =>
        pb.TransformFrameRequest(
          modelName = request.modelName,
          format = format,
          frame = ByteString.copyFrom(bytes),
          options = Some(request.options)
        )
    }).flatMap {
      r =>
        stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).transform(r)
    }.flatMap {
      response =>
        if (response.status == TransformStatus.STATUS_OK) {
          Future.successful(reader.fromBytes(response.frame.toByteArray))
        } else {
          Future.failed(new RuntimeException(response.error))
        }
    }
  }

  override def frameFlow[Tag: TagBytes](request: CreateFrameFlowRequest)
                                       (implicit timeout: FiniteDuration): Flow[(StreamTransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    val frameReader = FrameReader(request.format)

    val responseSource = GrpcAkkaStreams.source[TransformFrameResponse].mapMaterializedValue {
      observer =>
        val requestObserver = stub.transformFrameStream(observer)

        // Initialize the stream
        requestObserver.onNext(pb.TransformFrameRequest(
          modelName = request.modelName,
          streamName = request.streamName,
          flowConfig = Some(request.flowConfig),
          format = request.format,
          initTimeout = timeout.toMillis
        ))
        requestObserver
    }

    Flow.fromGraph {
      GraphDSL.create(responseSource) {
        implicit builder =>
          responseSource =>
            import GraphDSL.Implicits._

            val iteratorFlatten = builder.add {
              Flow[StreamObserver[pb.TransformFrameRequest]].flatMapConcat {
                observer => Source.repeat(observer)
              }
            }

            val frameFlow = builder.add(Flow[(StreamTransformFrameRequest, Tag)])
            val zip = builder.add(Zip[StreamObserver[pb.TransformFrameRequest], (StreamTransformFrameRequest, Tag)])

            val transformFlow = builder.add {
              Flow[(StreamObserver[pb.TransformFrameRequest], (StreamTransformFrameRequest, Tag))].to {
                Sink.foreachParallel[(StreamObserver[pb.TransformFrameRequest], (StreamTransformFrameRequest, Tag))](8) {
                  case (observer, (r, tag)) =>
                    Future {
                      FrameWriter(r.frame, request.format).toBytes().map {
                        frame =>
                          observer.onNext(pb.TransformFrameRequest(
                            tag = ByteString.copyFrom(implicitly[TagBytes[Tag]].toBytes(tag)),
                            frame = ByteString.copyFrom(frame)
                          ))
                      }
                    }
                }
              }
            }

            val responseFlow = builder.add {
              Flow[pb.TransformFrameResponse].map {
                response =>
                  val tryRow = if (response.error.nonEmpty) {
                    Failure(new TransformError(response.error, response.backtrace))
                  } else {
                    frameReader.fromBytes(response.frame.toByteArray)
                  }

                  (tryRow, implicitly[TagBytes[Tag]].fromBytes(response.tag.toByteArray))
              }
            }

            responseSource ~> responseFlow
            builder.materializedValue ~> iteratorFlatten
            iteratorFlatten ~> zip.in0
            frameFlow ~> zip.in1
            zip.out ~> transformFlow

            FlowShape(frameFlow.in, responseFlow.out)
      }
    }.mapMaterializedValue(_ => NotUsed)
  }

  override def rowFlow[Tag: TagBytes](request: CreateRowFlowRequest)
                                     (implicit timeout: FiniteDuration): Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    val reader = RowReader(request.inputSchema, request.format)
    val writer = RowWriter(request.outputSchema, request.format)

    val _responseSource = GrpcAkkaStreams.source[TransformRowResponse].mapMaterializedValue {
      observer =>
        val requestObserver = stub.transformRowStream(observer)

        // Initialize the stream
        requestObserver.onNext(pb.TransformRowRequest(
          modelName = request.modelName,
          streamName = request.streamName,
          format = request.format,
          flowConfig = Some(request.flowConfig),
          initTimeout = timeout.toMillis
        ))
        requestObserver
    }

    Flow.fromGraph {
      GraphDSL.create(_responseSource) {
        implicit builder =>
          responseSource =>
            import GraphDSL.Implicits._

            val iteratorFlatten = builder.add {
              Flow[StreamObserver[pb.TransformRowRequest]].flatMapConcat {
                observer => Source.repeat(observer)
              }
            }

            val rowFlow = builder.add(Flow[(StreamTransformRowRequest, Tag)])
            val zip = builder.add(Zip[StreamObserver[pb.TransformRowRequest], (StreamTransformRowRequest, Tag)])

            val transformFlow = builder.add {
              Flow[(StreamObserver[pb.TransformRowRequest], (StreamTransformRowRequest, Tag))].to {
                Sink.foreachParallel[(StreamObserver[pb.TransformRowRequest], (StreamTransformRowRequest, Tag))](8) {
                  case (observer, (r, tag)) =>
                    Future {
                      writer.toBytes(r.row).map {
                        row =>
                          observer.onNext(pb.TransformRowRequest(
                            tag = ByteString.copyFrom(implicitly[TagBytes[Tag]].toBytes(tag)),
                            row = ByteString.copyFrom(row)
                          ))
                      }
                    }
                }
              }
            }

            val responseFlow = builder.add {
              Flow[pb.TransformRowResponse].map {
                response =>
                  val r = if (response.status == TransformStatus.STATUS_OK) {
                    if (response.row.size == 0) { Try(None: Option[Row]) }
                    else { reader.fromBytes(response.row.toByteArray).map(Option(_)) }
                  } else {
                    Failure(new RuntimeException(response.error))
                  }

                  (r, implicitly[TagBytes[Tag]].fromBytes(response.tag.toByteArray))
              }
            }

            responseSource ~> responseFlow
            builder.materializedValue ~> iteratorFlatten
            iteratorFlatten ~> zip.in0
            rowFlow ~> zip.in1
            zip.out ~> transformFlow

            FlowShape(rowFlow.in, responseFlow.out)
      }
    }.mapMaterializedValue(_ => NotUsed)
  }

  override def close(): Unit = { }
}
