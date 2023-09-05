package ml.combust.mleap.executor.repository

import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.executor.testkit.TestUtil
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.runtime.transformer.Pipeline
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AsyncFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}

class RepositoryBundleLoaderSpec extends AsyncFunSpec with Matchers with BeforeAndAfterAll {

  import scala.language.implicitConversions

  val repo = new FileRepository()

  override def afterAll(): Unit = {
    repo.shutdown()
  }

  describe("repository bundle loader") {
    it("loads bundle successfully") {
      val bundleLoader = new RepositoryBundleLoader(repo, ExecutionContext.Implicits.global)
      val result: Future[Bundle[Transformer]] = bundleLoader.loadBundle(TestUtil.lrUri)
      result.map {
        bundle => bundle.root shouldBe a [Pipeline]
      }

    }
  }
}
