package ml.combust.mleap.executor

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import ml.combust.mleap.executor.testkit.{TestUtil, TransformServiceSpec}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._

class MleapExecutorSpec extends TestKit(ActorSystem("MleapExecutorSpec"))
  with TransformServiceSpec
  with BeforeAndAfterAll
  with ScalaFutures {

  override lazy val transformService: MleapExecutor = MleapExecutor(system)
  private val frame = TestUtil.frame
  override implicit val materializer: Materializer = ActorMaterializer()(system)

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 5.seconds, verifySystemShutdown = true)
  }
}
