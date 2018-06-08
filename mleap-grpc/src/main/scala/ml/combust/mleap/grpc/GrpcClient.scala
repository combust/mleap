package ml.combust.mleap.grpc

import java.net.URI
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source, Zip}
import ml.combust.mleap.executor._
import ml.combust.mleap.pb.{GetBundleMetaRequest, TransformFrameResponse, TransformRowResponse}
import ml.combust.mleap.pb.MleapGrpc.MleapStub
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import TypeConverters._
import akka.stream.FlowShape
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import ml.combust.bundle.dsl.BundleInfo
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.grpc.stream.GrpcAkkaStreams
import ml.combust.mleap.pb
import ml.combust.mleap.runtime.serialization._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

class GrpcClient(stub: MleapStub)
                (implicit ec: ExecutionContext) extends Client {
  private val format: String = BuiltinFormats.binary
  private val reader: FrameReader = FrameReader(format)

  override def getBundleMeta(uri: URI)
                            (implicit timeout: FiniteDuration = Client.defaultTimeout): Future[BundleMeta] = {
    stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).getBundleMeta(GetBundleMetaRequest(uri = uri.toString)).map {
      meta =>
        BundleMeta(info = BundleInfo.fromBundle(meta.bundle.get),
          inputSchema = meta.inputSchema.get,
          outputSchema = meta.outputSchema.get)
    }
  }

  override def transform(uri: URI, request: TransformFrameRequest)
                        (implicit timeout: FiniteDuration = Client.defaultTimeout): Future[DefaultLeapFrame] = {
    Future.fromTry {
      request.frame.flatMap(frame => FrameWriter(frame, format).toBytes()).map {
        bytes =>
          stub.withDeadlineAfter(timeout.toMillis, TimeUnit.MILLISECONDS).
            transformFrame(pb.TransformFrameRequest(
              uri = uri.toString,
              format = format,
              frame = ByteString.copyFrom(bytes),
              options = Some(request.options)
            )).flatMap {
            response =>
              Future.fromTry {
                if (response.error.nonEmpty) {
                  Failure(new TransformError(response.error, response.backtrace))
                } else {
                  reader.fromBytes(response.frame.toByteArray)
                }
              }
          }
      }
    }.flatMap(identity)
  }

  override def frameFlow[Tag: TagBytes](uri: URI,
                                        config: StreamConfig): Flow[(TransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    val frameReader = FrameReader(BuiltinFormats.binary)

    val responseSource = GrpcAkkaStreams.source[TransformFrameResponse].mapMaterializedValue {
      observer =>
        val requestObserver = stub.transformFrameStream(observer)

        // Initialize the stream
        requestObserver.onNext(pb.TransformFrameRequest(
          uri = uri.toString,
          streamConfig = Some(config),
          format = BuiltinFormats.binary
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

            val frameFlow = builder.add(Flow[(TransformFrameRequest, Tag)])
            val zip = builder.add(Zip[StreamObserver[pb.TransformFrameRequest], (TransformFrameRequest, Tag)])

            val transformFlow = builder.add {
              Flow[(StreamObserver[pb.TransformFrameRequest], (TransformFrameRequest, Tag))].to {
                Sink.foreachParallel[(StreamObserver[pb.TransformFrameRequest], (TransformFrameRequest, Tag))](8) {
                  case (observer, (request, tag)) =>
                    Future {
                      request.frame.flatMap(frame => FrameWriter(frame, BuiltinFormats.binary).toBytes()).map {
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

  override def rowFlow[Tag: TagBytes](uri: URI,
                                      spec: StreamRowSpec,
                                      config: StreamConfig): Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), NotUsed] = {
    val rowWriter = RowWriter(spec.schema, BuiltinFormats.binary)

    val _responseSource = GrpcAkkaStreams.source[TransformRowResponse].mapMaterializedValue {
      observer =>
        val requestObserver = stub.transformRowStream(observer)

        // Initialize the stream
        requestObserver.onNext(pb.TransformRowRequest(
          uri = uri.toString,
          schema = Some(spec.schema),
          options = Some(spec.options),
          streamConfig = Some(config),
          format = BuiltinFormats.binary
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

            val rowFlow = builder.add(Flow[(Try[Row], Tag)])
            val zip = builder.add(Zip[StreamObserver[pb.TransformRowRequest], (Try[Row], Tag)])

            val transformFlow = builder.add {
              Flow[(StreamObserver[pb.TransformRowRequest], (Try[Row], Tag))].to {
                Sink.foreachParallel[(StreamObserver[pb.TransformRowRequest], (Try[Row], Tag))](8) {
                  case (observer, (tryRow, tag)) =>
                    Future {
                      tryRow.flatMap(row => rowWriter.toBytes(row)).map {
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
              Flow[pb.TransformRowResponse].statefulMapConcat(
                () => {
                  var reader: Option[RowReader] = None

                  (response) => {
                    reader match {
                      case Some(r) =>
                        val tryRow = if (response.error.nonEmpty) {
                          Failure(new TransformError(response.error, response.backtrace))
                        } else if (response.row.isEmpty) {
                          Try(None)
                        } else {
                          r.fromBytes(response.row.toByteArray).map(row => Some(row))
                        }

                        scala.collection.immutable.Iterable((tryRow, implicitly[TagBytes[Tag]].fromBytes(response.tag.toByteArray)))
                      case None =>
                        for (schema <- response.schema) {
                          val sSchema: StructType = schema
                          reader = Some(RowReader(sSchema, format))
                        }
                        scala.collection.immutable.Iterable()
                    }
                  }
                }
              )
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
}
