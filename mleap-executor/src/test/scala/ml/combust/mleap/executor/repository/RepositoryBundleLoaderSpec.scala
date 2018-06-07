package ml.combust.mleap.executor.repository

import ml.combust.mleap.executor.TestUtil
import ml.combust.mleap.runtime.transformer.Pipeline
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext

class RepositoryBundleLoaderSpec extends FunSpec with ScalaFutures with Matchers {

  describe("repository bundle loader") {

    implicit val executionContext = ExecutionContext.Implicits.global

    it("loads bundle successfully") {
      val repo = new FileRepository()
      val bundleLoader = new RepositoryBundleLoader(repo, executionContext)
      val result = bundleLoader.loadBundle(TestUtil.lrUri)
      whenReady(result, Timeout(10.seconds)) {
        bundle => bundle.root shouldBe a [Pipeline]
      }

      repo.shutdown()
    }
  }
}
