package ml.combust.mleap.executor.service

import java.util.UUID

import akka.actor.{ActorSystem, Status}
import akka.testkit.{ImplicitSender, TestKit}
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.repository.{FileRepository, RepositoryBundleLoader}
import ml.combust.mleap.executor.service.BundleActor.{CreateFrameStream, CreateRowStream, GetBundleMeta}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, RowTransformer, Transformer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success
import TestUtil.{faultyFrame, frame, lrBundle, lrUri}

class BundleActorSpec extends TestKit(ActorSystem("BundleActorSpec"))
  with ImplicitSender
  with FunSpecLike
  with BeforeAndAfterAll
  with ScalaFutures {

  implicit val executionContext = ExecutionContext.Implicits.global

  override protected def afterAll(): Unit = {
    system.terminate()
  }

  describe("bundle actor") {

    val loader = new RepositoryBundleLoader(new FileRepository(false), executionContext)
    val service = new TransformService(loader, system)
    val actor = system.actorOf(BundleActor.props(service, lrUri, Future { lrBundle }))

    it("it retrieves bundle meta info correctly correctly") {
      actor ! GetBundleMeta

      val bundleMeta = expectMsgType[BundleMeta]
      assert(bundleMeta.info.name == "pipeline_ed5135e9ca49")
    }

    it("returns failure when error loading bundle") {
      val actor = system.actorOf(BundleActor.props(service, lrUri,
        Future { Thread.sleep(100); throw new IllegalArgumentException("invalid") }))
      actor ! GetBundleMeta

      expectMsgType[Status.Failure]
    }

    it("transforms valid frame successfully") {
      actor ! TransformFrameRequest(Success(frame))

      val transformed = expectMsgType[DefaultLeapFrame]
      assert(transformed.collect().size == 1)
    }

    it("errors on invalid frame transform") {
      actor ! TransformFrameRequest(Success(faultyFrame))
      expectMsgType[Status.Failure]
    }

    it("creates row transformer stream successfully") {
      actor ! CreateRowStream(UUID.randomUUID(), StreamRowSpec(frame.schema))

      val rowTransformer = expectMsgType[RowTransformer]
      frame.schema.fields.foreach(field => assert(rowTransformer.inputSchema.fields.contains(field)))
    }


    it("creates transformer stream successfully") {
      actor ! CreateFrameStream(UUID.randomUUID())

      val transformer = expectMsgType[Transformer]
      assert(lrBundle.root == transformer)
    }
  }
}
