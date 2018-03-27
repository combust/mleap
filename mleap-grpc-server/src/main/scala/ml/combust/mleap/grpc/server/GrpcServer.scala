package ml.combust.mleap.grpc.server

import java.net.URI

import io.grpc.stub.StreamObserver
import ml.combust.mleap
import ml.combust.mleap.executor.MleapExecutor
import ml.combust.mleap.pb._
import ml.combust.mleap.pb.MleapGrpc.Mleap
import ml.combust.mleap.runtime.serialization.{FrameReader, FrameWriter}
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import TypeConverters._
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

class GrpcServer(executor: MleapExecutor)
                (implicit ec: ExecutionContext) extends Mleap {
  override def getBundleMeta(request: GetBundleMetaRequest): Future[BundleMeta] = {
    executor.getBundleMeta(URI.create(request.uri)).map {
      meta =>
        BundleMeta(bundle = Some(meta.info.asBundle),
          inputSchema = Some(meta.inputSchema),
          outputSchema = Some(meta.outputSchema))
    }
  }

  override def transformFrame(request: TransformFrameRequest): Future[TransformFrameResult] = {
    Future.fromTry {
      FrameReader(request.format).fromBytes(request.frame.toByteArray).map {
        frame =>
          executor.transform(URI.create(request.uri), mleap.executor.TransformFrameRequest(frame, request.options)).flatMap {
            frame =>
              Future.fromTry(FrameWriter(frame, request.format).toBytes().map {
                bytes => TransformFrameResult(frame = ByteString.copyFrom(bytes), status = TransformStatus.STATUS_OK)
              })
          }
      }
    }.flatMap(identity).recover {
      case error => TransformFrameResult(status = TransformStatus.STATUS_ERROR, error = error.getMessage)
    }
  }

  override def transformRowStream(responseObserver: StreamObserver[TransformRowResult]): StreamObserver[TransformRowRequest] = {
    
  }
}
