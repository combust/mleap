package ml.combust.mleap.grpc.server


import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import io.grpc.{ManagedChannel, Server}
import ml.combust.mleap.executor.{Client, TransformFrameRequest}
import org.scalatest.{BeforeAndAfterEach, FunSpec}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class GrpcSpec extends FunSpec with BeforeAndAfterEach with ScalaFutures {

  var server : Server = _
  var client : Client = _
  var inProcessChannel : ManagedChannel = _
  var system : ActorSystem = _

  override def beforeEach() = {
    system = ActorSystem("grpc-server-test")
    inProcessChannel = TestUtil.inProcessChannel
    server = TestUtil.server(system)
    client = TestUtil.client(inProcessChannel)
  }

  override def afterEach() {
    inProcessChannel.shutdownNow
    server.shutdown
    system.terminate()
  }

  describe("grpc server and client") {
    it("retrieves bundle metadata") {
      val response = client.getBundleMeta(TestUtil.lrUri)
      whenReady(response, Timeout(5.seconds)) {
        meta => assert(meta.info.name == "pipeline_ed5135e9ca49")
      }
    }

    it("transforms frame") {
      client.getBundleMeta(TestUtil.lrUri)
      val response = client.transform(TestUtil.lrUri, TransformFrameRequest(TestUtil.frame)
                                      )(FiniteDuration(10, TimeUnit.SECONDS))
      whenReady(response, Timeout(5.seconds)) {
        frame => {
          val data = frame.dataset.toArray
          assert(data(0).getDouble(26) == 232.62463916840318)
        }
      }
    }
  }
}
