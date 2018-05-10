package ml.combust.mleap.grpc.server


import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import io.grpc.{ManagedChannel, Server}
import ml.combust.mleap.executor.{Client, TransformFrameRequest}
import org.scalatest.{BeforeAndAfterEach, FunSpec}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import ml.combust.mleap.grpc.server.TestUtil._

class GrpcSpec extends FunSpec with BeforeAndAfterEach with ScalaFutures {

  var server : Server = _
  var client : Client = _
  var channel : ManagedChannel = _
  var system : ActorSystem = _
  implicit val timeout = FiniteDuration(5, TimeUnit.SECONDS)

  override def beforeEach() = {
    system = ActorSystem("grpc-server-test")
    channel = inProcessChannel
    server = createServer(system)
    client = createClient(channel)
  }

  override def afterEach() {
    channel.shutdown
    server.shutdown
    system.terminate
  }

  describe("grpc server and client") {
    it("retrieves bundle metadata") {
      val response = client.getBundleMeta(lrUri)
      whenReady(response, Timeout(5.seconds)) {
        meta => assert(meta.info.name == "pipeline_ed5135e9ca49")
      }
    }

    it("transforms frame") {
      client.getBundleMeta(lrUri)
      val response = client.transform(lrUri, TransformFrameRequest(frame))
      whenReady(response, Timeout(5.seconds)) {
        frame => {
          val data = frame.dataset.toArray
          assert(data(0).getDouble(26) == 232.62463916840318)
        }
      }
    }
  }
}
