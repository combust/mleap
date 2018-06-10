package ml.combust.mleap.grpc

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

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
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import ml.combust.mleap.grpc.stream.GrpcAkkaStreams
import ml.combust.mleap.{executor, pb}
import ml.combust.mleap.runtime.serialization._

import scala.collection.concurrent.TrieMap
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

  override def getModel(request: GetModelRequest)
                       (implicit timeout: FiniteDuration): Future[Model] = {
    stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).
      getModel(request).
      map(pbToMleapModel)
  }

  override def getFrameStream(request: GetFrameStreamRequest)
                             (implicit timeout: FiniteDuration): Future[FrameStream] = {
    stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).
      getFrameStream(request).
      map(pbToMleapFrameStream)
  }

  override def getRowStream(request: GetRowStreamRequest)
                           (implicit timeout: FiniteDuration): Future[RowStream] = {
    stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).
      getRowStream(request).
      map(pbToMleapRowStream)
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
      r => stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).transform(r)
    }.flatMap {
      response =>
        if (response.status == TransformStatus.STATUS_OK) {
          Future.successful(reader.fromBytes(response.frame.toByteArray))
        } else {
          Future.failed(new RuntimeException(response.error))
        }
    }
  }

  override def createFrameFlow[Tag](request: CreateFrameFlowRequest)
                                   (implicit timeout: FiniteDuration): Flow[(StreamTransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    val lookup = TrieMap[Long, Tag]()
    val atomicIndex = new AtomicLong(0)
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

            val inFlow = builder.add {
              Flow[(StreamTransformFrameRequest, Tag)].map {
                case (r, tag) =>
                  val index = atomicIndex.incrementAndGet()
                  lookup += index -> tag

                  (r, index)
              }
            }
            val zip = builder.add(Zip[StreamObserver[pb.TransformFrameRequest], (StreamTransformFrameRequest, Long)])

            val transformFlow = builder.add {
              Flow[(StreamObserver[pb.TransformFrameRequest], (StreamTransformFrameRequest, Long))].to {
                Sink.foreach[(StreamObserver[pb.TransformFrameRequest], (StreamTransformFrameRequest, Long))] {
                  case (observer, (r, tag)) =>
                    Future {
                      r.frame.flatMap {
                        frame =>
                          FrameWriter(frame, request.format).toBytes()
                      }.map {
                        frame =>
                          observer.onNext(pb.TransformFrameRequest(
                            tag = tag,
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

                  (tryRow, lookup.remove(response.tag).get)
              }
            }

            responseSource ~> responseFlow
            builder.materializedValue ~> iteratorFlatten
            iteratorFlatten ~> zip.in0
            inFlow ~> zip.in1
            zip.out ~> transformFlow

            FlowShape(inFlow.in, responseFlow.out)
      }
    }.mapMaterializedValue(_ => NotUsed)
  }

  override def createRowFlow[Tag](request: CreateRowFlowRequest)
                                 (implicit timeout: FiniteDuration): Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    val lookup = TrieMap[Long, Tag]()
    val atomicIndex = new AtomicLong(0)
    val reader = RowReader(request.outputSchema, request.format)
    val writer = RowWriter(request.inputSchema, request.format)

    val _responseSource = GrpcAkkaStreams.source[TransformRowResponse].mapMaterializedValue {
      observer =>
        val requestObserver = stub.transformRowStream(observer)

        // Initialize the stream
        requestObserver.onNext(pb.TransformRowRequest(
          modelName = request.modelName,
          streamName = request.streamName,
          format = request.format,
          flowConfig = Some(request.flowConfig),
          initTimeout = timeout.toMillis,
          inputSchema = Some(request.inputSchema),
          outputSchema = Some(request.outputSchema)
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

            val inFlow = builder.add {
              Flow[(StreamTransformRowRequest, Tag)].map {
                case (r, tag) =>
                  val index = atomicIndex.incrementAndGet()
                  lookup += index -> tag

                  (r, index)
              }
            }
            val zip = builder.add(Zip[StreamObserver[pb.TransformRowRequest], (StreamTransformRowRequest, Long)])

            val transformFlow = builder.add {
              Flow[(StreamObserver[pb.TransformRowRequest], (StreamTransformRowRequest, Long))].to {
                Sink.foreach[(StreamObserver[pb.TransformRowRequest], (StreamTransformRowRequest, Long))] {
                  case (observer, (r, tag)) =>
                    Future {
                      r.row.flatMap {
                        row =>
                          writer.toBytes(row).map {
                            row =>
                              observer.onNext(pb.TransformRowRequest(
                                tag = tag,
                                row = ByteString.copyFrom(row)
                              ))
                          }
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

                  (r, lookup.remove(response.tag).get)
              }
            }

            responseSource ~> responseFlow
            builder.materializedValue ~> iteratorFlatten
            iteratorFlatten ~> zip.in0
            inFlow ~> zip.in1
            zip.out ~> transformFlow

            FlowShape(inFlow.in, responseFlow.out)
      }
    }.mapMaterializedValue(_ => NotUsed)
  }

  override def close(): Unit = { }
}
