package ml.combust.mleap.executor.testkit

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.service.TransformService
import ml.combust.mleap.runtime.frame.Row
import ml.combust.mleap.runtime.serialization.BuiltinFormats
import org.scalatest.FunSpecLike
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

trait TransformServiceSpec extends FunSpecLike
  with ScalaFutures {
  def transformService: TransformService
  implicit def materializer: Materializer

  private val frame = TestUtil.frame
  private val model1 = Model(
    name = "model1",
    uri = TestUtil.rfUri,
    config = ModelConfig(
      memoryTimeout = 15.minutes,
      diskTimeout = 15.minutes
    )
  )

  private val streamConfig1 = StreamConfig(
    idleTimeout = 15.minutes,
    transformTimeout = 15.minutes,
    parallelism = 4,
    throttle = None,
    bufferSize = 1024
  )
  private val rowStreamSpec1 = RowStreamSpec(
    format = BuiltinFormats.binary,
    schema = frame.schema
  )

  private val rowStream1 = RowStream(
    modelName = "model1",
    streamName = "stream1",
    streamConfig = streamConfig1,
    spec = rowStreamSpec1,
    outputSchema = TestUtil.rfRowTransformer.outputSchema
  )

  describe("when rf model is loaded") {
    val model = Await.result(transformService.loadModel(LoadModelRequest(
      modelName = "model1",
      uri = TestUtil.rfUri,
      config = ModelConfig(
        memoryTimeout = 15.minutes,
        diskTimeout = 15.minutes
      )
    ))(10.seconds), 10.seconds)

    val rowStream = Await.result(transformService.createRowStream(CreateRowStreamRequest(
      modelName = "model1",
      streamName = "stream1",
      streamConfig = streamConfig1,
      spec = rowStreamSpec1
    ))(10.seconds), 10.seconds)

    it("returns the model") {
      assert(model == model1)
    }

    it("returns the row stream") {
      assert(rowStream == rowStream1)
    }

    describe("get bundle meta") {
      it("retrieves info for bundle") {
        val result = transformService.getBundleMeta(GetBundleMetaRequest("model1"))(5.second)

        whenReady(result, Timeout(5.seconds)) {
          info => assert(info.info.name == "pipeline_8d2ca5c4dd62")
        }
      }
    }

    describe("get the model") {
      it("retrieves the model information") {
        val result = transformService.getModel(GetModelRequest("model1"))(5.second)

        whenReady(result, Timeout(5.seconds)) {
          model => assert(model == model1)
        }
      }
    }

    describe("get the row stream") {
      it("retrieves the row stream information") {
        val result = transformService.getRowStream(GetRowStreamRequest("model1", "stream1"))(5.second)

        whenReady(result, Timeout(5.seconds)) {
          rowStream => assert(rowStream == rowStream1)
        }
      }
    }

    describe("transforming a frame") {
      it("transforms a frame") {
        val result = transformService.transform(TransformFrameRequest("model1", frame))(5.seconds).
          flatMap(Future.fromTry)

        whenReady(result, Timeout(5.seconds)) {
          transformed => assert(transformed.schema.hasField("price_prediction"))
        }
      }
    }

    describe("transform row stream") {
      it("transforms rows in a stream") {
        val rowsSource = Source.fromIterator(() => frame.dataset.iterator.map(row => (StreamTransformRowRequest(Try(row)), UUID.randomUUID())))
        val rowsSink = Sink.seq[(Try[Option[Row]], UUID)]
        val testFlow = Flow.fromSinkAndSourceMat(rowsSink, rowsSource)(Keep.left)

        val config = FlowConfig(
          idleTimeout = 15.minutes,
          transformTimeout = 15.minutes,
          parallelism = 4,
          throttle = None
        )

        val rowFlow = transformService.createRowFlow[UUID](CreateRowFlowRequest("model1",
          "stream1",
          BuiltinFormats.binary,
          config,
          rowStream.spec.schema,
          rowStream.outputSchema))(10.seconds)

        val (done, transformedRows) = rowFlow.
          watchTermination()(Keep.right).
          joinMat(testFlow)(Keep.both).
          run()

        whenReady(transformedRows, Timeout(10.seconds)) {
          rows =>
            assert(rows.size == 1)

            for ((row, _) <- rows) {
              assert(row.isSuccess)
            }
        }

        done.isReadyWithin(1.second)
      }
    }
  }
}
