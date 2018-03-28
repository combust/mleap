package ml.combust.mleap.grpc

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source, Zip}
import ml.combust.mleap.executor._
import ml.combust.mleap.pb.{GetBundleMetaRequest, TransformRowResponse}
import ml.combust.mleap.pb.MleapGrpc.MleapStub
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import TypeConverters._
import akka.stream.FlowShape
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import ml.combust.bundle.dsl.BundleInfo
import ml.combust.mleap.grpc.stream.GrpcAkkaStreams
import ml.combust.mleap.pb
import ml.combust.mleap.runtime.serialization._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

class GrpcClient(stub: MleapStub)
                (implicit ec: ExecutionContext) extends Client {
  private val format: String = BuiltinFormats.binary
  private val reader: FrameReader = FrameReader(format)

  override def getBundleMeta(uri: URI): Future[BundleMeta] = {
    stub.getBundleMeta(GetBundleMetaRequest(uri = uri.toString)).map {
      meta =>
        BundleMeta(info = BundleInfo.fromBundle(meta.bundle.get),
          inputSchema = meta.inputSchema.get,
          outputSchema = meta.outputSchema.get)
    }
  }

  override def transform(uri: URI, request: TransformFrameRequest): Future[DefaultLeapFrame] = {
    Future.fromTry {
      request.frame.flatMap(frame => FrameWriter(frame, format).toBytes()).map {
        bytes =>
          stub.transformFrame(pb.TransformFrameRequest(
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

  override def rowFlow[Tag: TagBytes](uri: URI, spec: StreamRowSpec): Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), NotUsed] = {
    val rowReader = RowReader(spec.schema, BuiltinFormats.binary)
    val rowWriter = RowWriter(spec.schema, BuiltinFormats.binary)

    val responseSource = GrpcAkkaStreams.source[TransformRowResponse].mapMaterializedValue {
      observer =>
        val requestObserver = stub.transformRowStream(observer)

        // Initialize the stream
        requestObserver.onNext(pb.TransformRowRequest(
          uri = uri.toString,
          schema = Some(spec.schema),
          options = Some(spec.options)
        ))
        requestObserver
    }

    Flow.fromGraph {
      GraphDSL.create(responseSource) {
        implicit builder =>
          _ =>
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
              Flow[pb.TransformRowResponse].map {
                response =>
                  val tryRow = if (response.error.nonEmpty) {
                    Failure(new TransformError(response.error, response.backtrace))
                  } else if (response.row.isEmpty) {
                    Try(None)
                  } else {
                    rowReader.fromBytes(response.row.toByteArray).map(row => Some(row))
                  }

                  (tryRow, implicitly[TagBytes[Tag]].fromBytes(response.tag.toByteArray))
              }
            }

            builder.materializedValue ~> iteratorFlatten
            iteratorFlatten ~> zip.in0
            rowFlow ~> zip.in1
            zip.out ~> transformFlow

            FlowShape(rowFlow.in, responseFlow.out)
      }
    }.mapMaterializedValue(_ => NotUsed)
  }
}
