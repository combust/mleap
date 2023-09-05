package ml.combust.mleap.executor.repository

import java.io.File
import java.net.URI
import java.nio.file.Files

import ml.combust.mleap.executor.error.BundleException
import ml.combust.mleap.executor.testkit.TestUtil
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AsyncFunSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers

class FileRepositorySpec extends AsyncFunSpec
  with ScalaFutures
  with Matchers
  with BeforeAndAfterAll {
  val repository = new FileRepository()

  override protected def afterAll(): Unit = repository.shutdown()

  describe("downloading a local bundle") {
    it("returns the local file path") {
      val path = repository.downloadBundle(TestUtil.lrUri)
      path map {
        p =>
          val bytes = Files.readAllBytes(new File(TestUtil.lrUri.getPath).toPath)
          bytes should contain theSameElementsInOrderAs Files.readAllBytes(p)
      }
    }

    it("throws an exception when local file doesn't exist") {
      repository.downloadBundle(URI.create("does-not-exist")).failed map {
        ex => ex shouldBe a [BundleException]
      }
    }

    it("throws an exception with empty file path") {
      repository.downloadBundle(URI.create("")).failed map {
        ex => ex shouldBe a[BundleException]
      }
    }
  }
}
