package ml.combust.mleap.grpc.server

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import io.grpc.{ManagedChannel, Server}
import ml.combust.mleap.executor.service.TransformService
import ml.combust.mleap.executor.testkit.TransformServiceSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Ignore}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import ml.combust.mleap.grpc.server.TestUtil._

// Ignored temporarily because of random failures on Travis
@Ignore
class GrpcSpec extends TestKit(ActorSystem("grpc-server-test"))
  with TransformServiceSpec
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with ScalaFutures {

  private lazy val server = createServer(system)
  private lazy val channel = inProcessChannel
  private lazy val client = createClient(channel)

  override lazy val transformService: TransformService = {
    server
    client
  }

  override implicit def materializer: Materializer = ActorMaterializer()(system)

  override protected def afterAll(): Unit = {
    server.shutdown()
    channel.shutdown()
    TestKit.shutdownActorSystem(system, 5.seconds, verifySystemShutdown = true)
  }
}
