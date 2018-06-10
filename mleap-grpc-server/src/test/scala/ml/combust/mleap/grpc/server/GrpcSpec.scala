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
import ml.combust.mleap.runtime.serialization.BuiltinFormats

import scala.concurrent.Await
import scala.util.Try

class GrpcSpec extends TestKit(ActorSystem("grpc-server-test"))
  with FunSpecLike
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with ScalaFutures {

  var server: Server = createServer(system)
  var channel: ManagedChannel = inProcessChannel
  var client: GrpcClient = createClient(channel)
  implicit val timeout = FiniteDuration(5, TimeUnit.SECONDS)
  val frame = TestUtil.frame.get

  Await.result(
    client.loadModel(LoadModelRequest(
      modelName = "lr_model",
      uri = TestUtil.lrUri,
      config = ModelConfig(
        memoryTimeout = 15.minutes,
        diskTimeout = 15.minutes
      )
    ))(10.seconds), 10.seconds)

  val rowStreamConfig = StreamConfig(
    idleTimeout = 15.minutes,
    transformTimeout = 15.minutes,
    parallelism = 4,
    throttle = None,
    bufferSize = 1024
  )
  val spec = RowStreamSpec(BuiltinFormats.binary, frame.schema)

  val rowStream = Await.result(
    client.createRowStream(CreateRowStreamRequest(
      "lr_model",
      "stream1",
      rowStreamConfig,
      spec
    ))(10.seconds), 10.seconds)

  val flowConfig = FlowConfig(
    idleTimeout = 15.minutes,
    transformTimeout = 15.minutes,
    parallelism = 4,
    throttle = None
  )

  override protected def afterAll(): Unit = {
    channel.shutdown
    server.shutdown
    system.terminate()
  }

  describe("grpc server and client") {
    it("retrieves bundle metadata") {
      val response = client.getBundleMeta(GetBundleMetaRequest("lr_model"))
      whenReady(response, Timeout(5.seconds)) {
        meta => assert(meta.info.name == "pipeline_ed5135e9ca49")
      }
    }

    it("transforms frame") {
      val response = client.transform(TransformFrameRequest("lr_model", frame))
      whenReady(response, Timeout(5.seconds)) {
        frame => {
          val data = frame.get.dataset.toArray
          assert(data(0).getDouble(26) == 232.62463916840318)
        }
      }
    }

    it("transforms a row using row flow") {
      val uuid = UUID.randomUUID()
      val stream = Source.fromIterator(() => frame.dataset.iterator.map(row => (StreamTransformRowRequest(Try(row)), uuid)))
        .via(client.createRowFlow[UUID](CreateRowFlowRequest(
          modelName = "lr_model",
          streamName = "stream1",
          format = BuiltinFormats.binary,
          flowConfig = flowConfig,
          inputSchema = rowStream.spec.schema,
          outputSchema = rowStream.outputSchema
        ))).toMat(TestSink.probe(system))(Keep.right)
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

      val done = Source.single((StreamTransformRowRequest(Try(frame.dataset.head)), uuid))
        .via(client.createRowFlow[UUID](CreateRowFlowRequest(
          modelName = "lr_model",
          streamName = "stream1",
          format = BuiltinFormats.binary,
          flowConfig = flowConfig,
          inputSchema = rowStream.spec.schema,
          outputSchema = rowStream.outputSchema
        ))).watchTermination()(Keep.right)
        .to(Sink.ignore)
        .run()(ActorMaterializer.create(system))

      done.isReadyWithin(2.seconds)
    }

    it("returns error with the right exception when creating row stream") {
      val response = client.createRowStream(CreateRowStreamRequest(
        "lr_model",
        "stream2",
        rowStreamConfig,
        spec = RowStreamSpec(
          format = BuiltinFormats.binary,
          schema = frame.schema,
          options = TransformOptions(select = Some(Seq("dummy1", "dummy2")))
        )
      ))(10.seconds)

      whenReady(response.failed) {
        ex =>
          assert(ex.isInstanceOf[StatusRuntimeException])
          val sre = ex.asInstanceOf[StatusRuntimeException]
          assert(sre.getStatus.getCode == Status.FAILED_PRECONDITION.getCode)
          assert(sre.getStatus.getDescription == "invalid fields: dummy1,dummy2")
      }
    }
    //
    //    it("transforms a frame using a frame flow") {
    //      val uuid = UUID.randomUUID()
    //      val source = Source.fromIterator(() => Iterator.apply(frame).map(f => (TransformFrameRequest(f), uuid)))
    //      val result = source
    //        .via(client.frameFlow(lrUri, streamConfig))
    //        .runWith(TestSink.probe(system))(ActorMaterializer.create(system))
    //        .request(1)
    //        .expectNext() // throws exception, might need same fixes from rowFlow?
    //      assert(result._2 == uuid)
    //      assert(result._1.isSuccess)
    //      val row = result._1.get.dataset.toArray.apply(0)
    //      assert(row.getDouble(26) == 232.62463916840318)
    //    }
    //
    //    it("shuts down the frame flow when upstream completes") {
    //      val uuid = UUID.randomUUID()
    //      val done = Source.single((TransformFrameRequest(frame), uuid))
    //        .via(client.frameFlow(lrUri, streamConfig))
    //        .watchTermination()(Keep.right)
    //        .to(Sink.ignore)
    //        .run()(ActorMaterializer.create(system))
    //
    //      done.isReadyWithin(2.seconds)
    //    }
  }
}
