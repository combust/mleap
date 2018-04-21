package ml.combust.mleap.executor.repository

import java.io.File
import java.net.URI
import java.nio.file.Files

import ml.combust.mleap.executor.TestUtil

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

class FileRepositorySpec extends FunSpec with ScalaFutures with Matchers {
  val repository = new FileRepository(true)

  describe("downloading a local bundle") {
    it("returns the local file path") {
      val path = repository.downloadBundle(TestUtil.lrUri)

      whenReady(path) {
        p => assert(Files.readAllBytes(new File(TestUtil.lrUri.getPath).toPath).sameElements(Files.readAllBytes(p)))
      }
    }

    it("throws an exception when local file doesn't exist") {
      whenReady(repository.downloadBundle(URI.create("does-not-exist")).failed) {
        ex => ex shouldBe a [IllegalArgumentException]
      }
    }

    it("throws an exception with empty file path") {
      whenReady(repository.downloadBundle(URI.create("")).failed) {
        ex => ex shouldBe a [IllegalArgumentException]
      }
    }
  }
}
