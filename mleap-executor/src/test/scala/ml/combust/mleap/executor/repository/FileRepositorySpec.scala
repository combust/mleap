package ml.combust.mleap.executor.repository

import java.io.File
import java.nio.file.Files

import org.scalatest.FunSpec
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.ExecutionContext.Implicits.global

class FileRepositorySpec extends FunSpec with ScalaFutures {
  val repository = new FileRepository(true)

  describe("downloading a local bundle") {
    it("returns the local file path") {
      val local = getClass.getClassLoader.getResource("models/airbnb.model.rf.zip").toURI
      val path = repository.downloadBundle(local)

      whenReady(path) {
        p => assert(Files.readAllBytes(new File(local.getPath).toPath).sameElements(Files.readAllBytes(p)))
      }
    }
  }
}
