package ml.combust.mleap.executor.repository

import java.io.File
import java.net.URI
import java.nio.file.Files

import ml.combust.mleap.executor.error.BundleException
import ml.combust.mleap.executor.testkit.TestUtil
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

class FileRepositorySpec extends FunSpec
  with ScalaFutures
  with Matchers
  with BeforeAndAfterAll {
  val repository = new FileRepository()

  override protected def afterAll(): Unit = repository.shutdown()

  describe("downloading a local bundle") {
    it("returns the local file path") {
      val path = repository.downloadBundle(TestUtil.lrUri)

      whenReady(path) {
        p => assert(Files.readAllBytes(new File(TestUtil.lrUri.getPath).toPath).sameElements(Files.readAllBytes(p)))
      }
    }

    it("throws an exception when local file doesn't exist") {
      whenReady(repository.downloadBundle(URI.create("does-not-exist")).failed) {
        ex => ex shouldBe a [BundleException]
      }
    }

    it("throws an exception with empty file path") {
      whenReady(repository.downloadBundle(URI.create("")).failed) {
        ex => ex shouldBe a [BundleException]
      }
    }
  }
}
