package ml.combust.mleap.grpc.server


import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import io.grpc.{ManagedChannel, Server, Status, StatusRuntimeException}
import ml.combust.mleap.executor._
import ml.combust.mleap.grpc.GrpcClient
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
  var client : GrpcClient = _
  var channel : ManagedChannel = _
  implicit val timeout = FiniteDuration(5, TimeUnit.SECONDS)
  val streamConfig = StreamConfig(
    idleTimeout = 10.seconds,
    transformTimeout = 10.seconds,
    parallelism = 4,
    bufferSize = 1024
  )

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
      val response = client.getBundleMeta(GetBundleMetaRequest())
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
      val stream = Source.fromIterator(() => frame.get.dataset.iterator.map(row => (Try(row), uuid)))
        .via(client.rowFlow(lrUri, RowStreamSpec(frame.get.schema), streamConfig))
        .toMat(TestSink.probe(system))(Keep.right)
        .run()(ActorMaterializer.create(system))

      val result = stream
        .request(1)
        .expectNext()
      assert(result._2 == uuid)
      assert(result._1.isSuccess)
      val row = result._1.get.get
      assert(row.getDouble(26) == 232.62463916840318)
    }

    it("shuts down the row stream when upstream completes") {
      val uuid = UUID.randomUUID()

      val done = Source.single((Try(frame.get.dataset.head), uuid))
        .via(client.rowFlow(lrUri, RowStreamSpec(frame.get.schema), streamConfig))
        .watchTermination()(Keep.right)
        .to(Sink.ignore)
        .run()(ActorMaterializer.create(system))

      done.isReadyWithin(2.seconds)
    }

    it("returns error with the right exception when using row flow"){
      val ex = Source.fromIterator(() => frame.get.dataset.iterator.map(row => (Try(row), UUID.randomUUID())))
        .via(client.rowFlow(lrUri, RowStreamSpec(frame.get.schema, TransformOptions(Some(Seq("dummy1", "dummy2")))), streamConfig))
        .runWith(TestSink.probe(system))(ActorMaterializer.create(system))
        .request(1)
        .expectError()

      assert(ex.isInstanceOf[StatusRuntimeException])
      val sre = ex.asInstanceOf[StatusRuntimeException]
      assert(sre.getStatus.getCode == Status.FAILED_PRECONDITION.getCode)
      assert(sre.getStatus.getDescription == "invalid fields: dummy1,dummy2")
    }

    it("transforms a frame using a frame flow") {
      val uuid = UUID.randomUUID()
      val source = Source.fromIterator(() => Iterator.apply(frame).map(f => (TransformFrameRequest(f), uuid)))
      val result = source
        .via(client.frameFlow(lrUri, streamConfig))
        .runWith(TestSink.probe(system))(ActorMaterializer.create(system))
        .request(1)
        .expectNext() // throws exception, might need same fixes from rowFlow?
      assert(result._2 == uuid)
      assert(result._1.isSuccess)
      val row = result._1.get.dataset.toArray.apply(0)
      assert(row.getDouble(26) == 232.62463916840318)
    }

    it("shuts down the frame flow when upstream completes") {
      val uuid = UUID.randomUUID()
      val done = Source.single((TransformFrameRequest(frame), uuid))
        .via(client.frameFlow(lrUri, streamConfig))
        .watchTermination()(Keep.right)
        .to(Sink.ignore)
        .run()(ActorMaterializer.create(system))
      
      done.isReadyWithin(2.seconds)
    }
  }
}
