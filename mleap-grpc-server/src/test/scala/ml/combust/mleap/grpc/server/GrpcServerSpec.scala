package ml.combust.mleap.grpc.server

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, Server}
import ml.combust.mleap.pb.{GetBundleMetaRequest, MleapGrpc, TransformFrameRequest}
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameWriter}
import org.scalatest.{BeforeAndAfterEach, FunSpec}

import scala.concurrent.duration.FiniteDuration

class GrpcServerSpec extends FunSpec with BeforeAndAfterEach {

  var server : Server = _
  var inProcessChannel : ManagedChannel = _
  var system : ActorSystem = _
  val format: String = BuiltinFormats.binary

  override def beforeEach() = {
    system = ActorSystem("grpc-server-test")
    inProcessChannel = TestUtil.inProcessChannel
    server = TestUtil.server(system)
  }

  override def afterEach() {
    inProcessChannel.shutdownNow
    server.shutdown
    system.terminate()
  }

  describe("grpc server with blocking client stub") {
    it("retrieves bundle metadata") {
      val stub = MleapGrpc.blockingStub(inProcessChannel)
      val meta = stub.getBundleMeta(GetBundleMetaRequest(uri = TestUtil.lrUri))
      assert(meta.getBundle.name == "pipeline_ed5135e9ca49")
    }

    it("transforms frame") {
      val frame = ByteString.copyFrom(FrameWriter(TestUtil.frame, format).toBytes().get)
      val stub = MleapGrpc.blockingStub(inProcessChannel)
      stub.getBundleMeta(GetBundleMetaRequest(uri = TestUtil.lrUri))

      val response = stub.transformFrame(
        TransformFrameRequest(uri = TestUtil.lrUri,
                              format = format,
                              timeout = FiniteDuration(1, TimeUnit.SECONDS).toMillis,
                              frame = frame,
                              options = None))
      assert(response.status.isStatusOk)
    }
  }

  describe("grpc server with a non-blocking client") {
  }

}
