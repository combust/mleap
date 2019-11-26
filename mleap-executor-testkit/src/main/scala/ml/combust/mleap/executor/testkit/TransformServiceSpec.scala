package ml.combust.mleap.executor.testkit

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.{Materializer, ThrottleMode}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.error.{AlreadyExistsException, NotFoundException, TimeoutException}
import ml.combust.mleap.executor.service.TransformService
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.serialization.BuiltinFormats
import org.scalatest.{FunSpecLike}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

trait TransformServiceSpec extends FunSpecLike
  with ScalaFutures {
  def transformService: TransformService
  implicit def system: ActorSystem
  implicit def materializer: Materializer

  private val frame = TestUtil.frame
  private val model1 = Model(
    name = "model1",
    uri = TestUtil.rfUri,
    config = ModelConfig(
      memoryTimeout = Some(15.minutes),
      diskTimeout = Some(15.minutes)
    )
  )

  private val streamConfig1 = StreamConfig(
    idleTimeout = None,
    transformDelay = Some(20.millis),
    parallelism = Some(4),
    throttle = None,
    bufferSize = Some(1024)
  )
  private val rowStreamSpec1 = RowStreamSpec(
    schema = frame.schema
  )

  private val rowStream1 = RowStream(
    modelName = "model1",
    streamName = "stream1",
    streamConfig = streamConfig1,
    spec = rowStreamSpec1,
    outputSchema = TestUtil.rfRowTransformer.outputSchema
  )

  private val frameStream1 = FrameStream(
    modelName = "model1",
    streamName = "stream2",
    streamConfig = streamConfig1
  )

  private val flowConfig = FlowConfig(
    idleTimeout = None,
    transformDelay = None,
    parallelism = Some(4),
    throttle = None
  )

  describe("when rf model is loaded") {
    val model = Await.result(transformService.loadModel(LoadModelRequest(
      modelName = "model1",
      uri = TestUtil.rfUri,
      config = Some(ModelConfig(
        memoryTimeout = Some(15.minutes),
        diskTimeout = Some(15.minutes)
      ))
    ))(10.seconds), 10.seconds)

    val rowStream = Await.result(transformService.createRowStream(CreateRowStreamRequest(
      modelName = "model1",
      streamName = "stream1",
      streamConfig = Some(streamConfig1),
      spec = rowStreamSpec1
    ))(10.seconds), 10.seconds)

    val frameStream = Await.result(transformService.createFrameStream(CreateFrameStreamRequest(
      modelName = "model1",
      streamName = "stream2",
      streamConfig = Some(streamConfig1)
    ))(10.seconds), 10.seconds)

    it("returns the model") {
      assert(model == model1)
    }

    it("returns the row stream") {
      assert(rowStream == rowStream1)
    }

    it("returns the frame stream") {
      assert(frameStream == frameStream1)
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

      describe("when row stream does not exist") {
        it("throws a NotFoundException") {
          val ex = transformService.getRowStream(GetRowStreamRequest("model1", "unknown"))(5.seconds).failed

          whenReady(ex) {
            e => assert(e.isInstanceOf[NotFoundException])
          }
        }
      }
    }

    describe("trying to create a row stream with the same name") {
      it("throws an AlreadyExistsException") {
        val ex = transformService.createRowStream(CreateRowStreamRequest(
          modelName = "model1",
          streamName = "stream1",
          streamConfig = Some(streamConfig1),
          spec = rowStreamSpec1
        ))(5.seconds).failed

        whenReady(ex) {
          e => assert(e.isInstanceOf[AlreadyExistsException])
        }
      }
    }

    describe("get the frame stream") {
      it("retrieves the frame stream information") {
        val result = transformService.getFrameStream(GetFrameStreamRequest("model1", "stream2"))(5.second)

        whenReady(result, Timeout(5.seconds)) {
          frameStream => assert(frameStream == frameStream1)
        }
      }

      describe("when frame stream does not exist") {
        it("throws a NotFoundException") {
          val ex = transformService.getFrameStream(GetFrameStreamRequest("model1", "unknown"))(5.seconds).failed

          whenReady(ex) {
            e => assert(e.isInstanceOf[NotFoundException])
          }
        }
      }
    }

    describe("trying to create a frame stream with the same name") {
      it("throws an AlreadyExistsException") {
        val ex = transformService.createFrameStream(CreateFrameStreamRequest(
          modelName = "model1",
          streamName = "stream2",
          streamConfig = Some(streamConfig1)
        ))(5.seconds).failed

        whenReady(ex) {
          e => assert(e.isInstanceOf[AlreadyExistsException])
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
        val uuid = UUID.randomUUID()
        val rowSource = Source.single((StreamTransformRowRequest(Try(frame.dataset.head)), uuid))
        val rowsSink = Sink.head[(Try[Option[Row]], UUID)]
        val testFlow = Flow.fromSinkAndSourceMat(rowsSink, rowSource)(Keep.left)

        val rowFlow = transformService.createRowFlow[UUID](CreateRowFlowRequest("model1",
          "stream1",
          BuiltinFormats.binary,
          Some(flowConfig),
          rowStream.spec.schema,
          rowStream.outputSchema))(10.seconds)

        val (done, transformedRow) = rowFlow.
          watchTermination()(Keep.right).
          joinMat(testFlow)(Keep.both).
          run()

        whenReady(transformedRow, Timeout(10.seconds)) {
          row =>
            assert(row._1.get.get.getDouble(21) == 218.2767196535019)
            assert(row._2 == uuid)
        }

        done.isReadyWithin(1.second)
      }

      describe("throttling") {
        // TODO this test fails randomly on Travis, so temporarily replaced it() with ignore()
        //  https://github.com/combust/mleap/issues/573
        ignore("throttles elements through the flow") {
          Source.fromIterator(
            () => Seq(
              (StreamTransformRowRequest(Try(frame.dataset.head)), 2),
              (StreamTransformRowRequest(Try(frame.dataset.head)), 3)
            ).iterator
          ).via(transformService.createRowFlow[Int](
            CreateRowFlowRequest("model1",
              "stream1",
              BuiltinFormats.binary,
              Some(flowConfig.copy(
                parallelism = Some(1),
                throttle = Some(
                  Throttle(
                    elements = 1,
                    duration = 1.day,
                    maxBurst = 1,
                    mode = ThrottleMode.shaping
                  )
                ))),
              rowStream.spec.schema,
              rowStream.outputSchema)
          )(10.seconds)).map(_._2).
            runWith(TestSink.probe[Int]).
            request(2).
            expectNext(2).
            expectNoMessage(100.millis).
            cancel()
        }
      }

      describe("idling") {
        it("closes the flow") {
          val err = Source.fromIterator(
            () => Seq(
              (StreamTransformRowRequest(Try(frame.dataset.head)), 2),
              (StreamTransformRowRequest(Try(frame.dataset.head)), 3)
            ).iterator
          ).initialDelay(100.millis).via(transformService.createRowFlow[Int](
            CreateRowFlowRequest("model1",
              "stream1",
              BuiltinFormats.binary,
              Some(flowConfig.copy(idleTimeout = Some(10.millis))),
              rowStream.spec.schema,
              rowStream.outputSchema)
          )(10.seconds)).map(_._2).
            runWith(TestSink.probe[Int]).
            request(2).
            expectError()

          assert(err.isInstanceOf[TimeoutException])
          assert(err.getMessage.contains("No elements passed in the last"))
        }
      }

      describe("transform delay") {
        it("delays the transform operation") {
          Source.fromIterator(
            () => Seq(
              (StreamTransformRowRequest(Try(frame.dataset.head)), 2),
              (StreamTransformRowRequest(Try(frame.dataset.head)), 3)
            ).iterator
          ).via(transformService.createRowFlow[Int](
            CreateRowFlowRequest("model1",
              "stream1",
              BuiltinFormats.binary,
              Some(flowConfig.copy(transformDelay = Some(1.day))),
              rowStream.spec.schema,
              rowStream.outputSchema)
          )(10.seconds)).map(_._2).
            runWith(TestSink.probe[Int]).
            request(2).
            expectNoMessage(100.millis).
            cancel()
        }
      }
    }

    describe("RowTransformClient") {
      it("transforms rows") {
        val client = transformService.createTransformRowClient(
          CreateRowFlowRequest("model1",
            "stream1",
            BuiltinFormats.binary,
            Some(flowConfig),
            rowStream.spec.schema,
            rowStream.outputSchema),
          1024
        )(10.seconds, materializer, system.dispatcher)

        whenReady(client.transform(frame.dataset.head)) {
          row =>
            assert(row.isSuccess)
        }

        whenReady(client.transform(frame.dataset.head)) {
          row =>
            assert(row.isSuccess)
        }
      }
    }

    describe("transform frame stream") {
      it("transforms frames in a stream") {
        val uuid = UUID.randomUUID()
        val rowSource = Source.single(StreamTransformFrameRequest(Try(frame), TransformOptions.default), uuid)
        val rowsSink = Sink.head[(Try[DefaultLeapFrame], UUID)]
        val testFlow = Flow.fromSinkAndSourceMat(rowsSink, rowSource)(Keep.left)

        val frameFlow = transformService.createFrameFlow[UUID](CreateFrameFlowRequest("model1",
          "stream2",
          BuiltinFormats.binary,
          Some(flowConfig)))(10.seconds)

        val (done, transformedFrame) = frameFlow.
          watchTermination()(Keep.right).
          joinMat(testFlow)(Keep.both).
          run()

        whenReady(transformedFrame, Timeout(10.seconds)) {
          frame =>
            assert(frame._1.get.dataset.head.getDouble(21) == 218.2767196535019)
            assert(frame._2 == uuid)
        }

        done.isReadyWithin(1.second)
      }

      describe("throttling") {
        // TODO this test fails randomly on Travis, so temporarily replaced it() with ignore()
        //  https://github.com/combust/mleap/issues/573
        ignore("throttles elements through the flow") {
          Source.fromIterator(
            () => Seq(
              (StreamTransformFrameRequest(Try(frame), TransformOptions.default), 1),
              (StreamTransformFrameRequest(Try(frame), TransformOptions.default), 2)
            ).iterator
          ).via(transformService.createFrameFlow[Int](
            CreateFrameFlowRequest("model1",
              "stream2",
              BuiltinFormats.binary,
              Some(flowConfig.copy(
                parallelism = Some(1),
                throttle = Some(
                  Throttle(
                    elements = 1,
                    duration = 1.day,
                    maxBurst = 1,
                    mode = ThrottleMode.shaping
                  )
                ))
              ))
          )(10.seconds)).map(_._2).
            runWith(TestSink.probe[Int]).
            request(2).
            expectNext(1).
            expectNoMessage(100.millis).
            cancel()
        }
      }

      describe("idling") {
        it("closes the flow") {
          val err = Source.fromIterator(
            () => Seq(
              (StreamTransformFrameRequest(Try(frame), TransformOptions.default), 1),
              (StreamTransformFrameRequest(Try(frame), TransformOptions.default), 2)
            ).iterator
          ).initialDelay(100.millis).via(transformService.createFrameFlow[Int](
            CreateFrameFlowRequest("model1",
              "stream2",
              BuiltinFormats.binary,
              Some(flowConfig.copy(idleTimeout = Some(10.millis))))
          )(10.seconds)).map(_._2).
            runWith(TestSink.probe[Int]).
            request(2).
            expectError()

          assert(err.isInstanceOf[TimeoutException])
          assert(err.getMessage.contains("No elements passed in the last"))
        }
      }

      describe("transform delay") {
        it("delays the transform operation") {
          Source.fromIterator(
            () => Seq(
              (StreamTransformFrameRequest(Try(frame), TransformOptions.default), 1),
              (StreamTransformFrameRequest(Try(frame), TransformOptions.default), 2)
            ).iterator
          ).via(transformService.createFrameFlow[Int](
            CreateFrameFlowRequest("model1",
              "stream2",
              BuiltinFormats.binary,
              Some(flowConfig.copy(transformDelay = Some(1.day))))
          )(10.seconds)).map(_._2).
            runWith(TestSink.probe[Int]).
            request(2).
            expectNoMessage(100.millis).
            cancel()
        }
      }
    }
  }

  describe("when model not loaded") {
    describe("get model") {
      it("returns a NotFoundException") {
        val ex = transformService.getModel(GetModelRequest("unknown"))(10.seconds).failed

        whenReady(ex) {
          e =>
            assert(e.isInstanceOf[NotFoundException])
        }
      }
    }

    describe("get frame stream") {
      it("returns a NotFoundException") {
        val ex = transformService.getFrameStream(GetFrameStreamRequest("unknown", "unknown"))(10.seconds).failed

        whenReady(ex) {
          e => assert(e.isInstanceOf[NotFoundException])
        }
      }
    }

    describe("get row stream") {
      it("returns a NotFoundException") {
        val ex = transformService.getRowStream(GetRowStreamRequest("unknown", "unknown"))(10.seconds).failed

        whenReady(ex) {
          e => assert(e.isInstanceOf[NotFoundException])
        }
      }
    }
  }
}
