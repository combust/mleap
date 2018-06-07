package ml.combust.mleap.executor

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.testkit.TestKit
import ml.combust.mleap.runtime.frame.Row
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}

import scala.concurrent.duration._
import scala.util.Try

class MleapExecutorSpec extends TestKit(ActorSystem("MleapExecutorSpec"))
  with FunSpecLike
  with BeforeAndAfterAll
  with ScalaFutures {

  private val executor = MleapExecutor(system)
  private val frame = TestUtil.frame
  private implicit val materializer: Materializer = ActorMaterializer()(system)

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 5.seconds, verifySystemShutdown = true)
  }

  describe("transforming a leap frame") {
    it("transforms the leap frame") {
      val result = executor.transform(TestUtil.rfUri, Try(frame))(5.second)

      whenReady(result, Timeout(5.seconds)) {
        transformed => assert(transformed.schema.hasField("price_prediction"))
      }
    }
  }

  describe("get bundle meta") {
    it("retrieves info for bundle") {
      val result = executor.getBundleMeta(TestUtil.lrUri)(5.second)
      whenReady(result, Timeout(5.seconds)) {
        info => assert(info.info.name == "pipeline_ed5135e9ca49")
      }
    }
  }

  describe("transform stream") {
    it("transforms rows in a stream") {
      val spec = StreamRowSpec(frame.schema)
      val rowsSource = Source.fromIterator(() => frame.dataset.iterator.map(row => Try(row)).zipWithIndex)
      val rowsSink = Sink.seq[(Try[Option[Row]], Int)]
      val testFlow = Flow.fromSinkAndSourceMat(rowsSink, rowsSource)(Keep.left)

      val transformedRows = executor.rowFlow(TestUtil.rfUri, spec)(10.seconds, 1).joinMat(testFlow)(Keep.right).run()

      whenReady(transformedRows, Timeout(10.seconds)) {
        rows =>
          for((row, _) <- rows) {
            assert(row.isSuccess)
          }
      }
    }
  }
}
