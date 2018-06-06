package ml.combust.mleap.grpc.server


import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import io.grpc.{ManagedChannel, Server}
import ml.combust.mleap.executor.{Client, StreamRowSpec, TransformFrameRequest, TransformOptions}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import ml.combust.mleap.grpc.server.TestUtil._

import scala.util.Try

class GrpcSpec extends TestKit(ActorSystem("grpc-server-test"))
  with FunSpecLike
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with ScalaFutures {

  var server : Server = _
  var client : Client = _
  var channel : ManagedChannel = _
  implicit val timeout = FiniteDuration(5, TimeUnit.SECONDS)

  override def beforeEach() = {
    channel = inProcessChannel
    server = createServer(system)
    client = createClient(channel)
  }

  override def afterEach() {
    channel.shutdown
    server.shutdown
  }

  override protected def afterAll(): Unit = {
    system.terminate()
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

    it("transforms a row using row flow") {
      val uuid = UUID.randomUUID()
      val result = Source.fromIterator(() => frame.get.dataset.iterator.map(row => (Try(row), uuid)))
        .via(client.rowFlow(lrUri, StreamRowSpec(frame.get.schema)))
        .runWith(TestSink.probe(system))(ActorMaterializer.create(system))
        .request(1)
        .expectNext()
      assert(result._2 == uuid)
      assert(result._1.isSuccess)
      val row = result._1.get.get
      assert(row.getDouble(26) == 232.62463916840318)
      // the issue i think is that this is still only the input row, not the transformed row
    }

    ignore("returns error with the right exception when using row flow"){
      val ex = Source.fromIterator(() => frame.get.dataset.iterator.map(row => (Try(row), UUID.randomUUID())))
        .via(client.rowFlow(lrUri, StreamRowSpec(frame.get.schema, TransformOptions(Some(Seq("dummy"))))))
        .runWith(TestSink.probe(system))(ActorMaterializer.create(system))
        .request(1)
        .expectError()
      assert(ex.isInstanceOf[IllegalArgumentException])
      // i thought ex would be IllegalArgumentException from the strict select?
      // instead it's io.grpc.StatusRuntimeException: UNKNOWN
    }

    ignore("transforms a frame using a frame flow") {
      val uuid = UUID.randomUUID()
      val source = Source.fromIterator(() => Iterator.apply(frame).map(f => (TransformFrameRequest(f), uuid)))
      val result = source
        .via(client.frameFlow(uri = lrUri))
        .runWith(TestSink.probe(system))(ActorMaterializer.create(system))
        .request(1)
        .expectNext() // throws exception, might need same fixes from rowFlow?
      assert(result._2 == uuid)
      assert(result._1.isSuccess)
      val row = result._1.get.dataset.toArray.apply(0)
      assert(row.getDouble(26) == 232.62463916840318)
    }
  }
}
