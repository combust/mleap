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
    system.terminate()
  }

  describe("transforming a leap frame") {
    it("transforms the leap frame") {
      val result = executor.transform(TestUtil.rfUri, frame)(5.second)

      whenReady(result, Timeout(5.seconds)) { _ => Unit }
    }
  }

  describe("transform stream") {
    it("transforms rows in a stream") {
      val spec = StreamRowSpec(frame.schema)
      val rowsSource = Source.fromIterator(() => frame.dataset.iterator.zipWithIndex)
      val rowsSink = Sink.seq[(Try[Option[Row]], Int)]
      val testFlow = Flow.fromSinkAndSourceMat(rowsSink, rowsSource)(Keep.left)

      val transformedRows = executor.rowFlow(TestUtil.rfUri, spec)(10.seconds).joinMat(testFlow)(Keep.right).run()

      whenReady(transformedRows, Timeout(10.seconds)) {
        rows =>
          for((row, _) <- rows) {
            assert(row.isSuccess)
          }
      }
    }
  }
}
