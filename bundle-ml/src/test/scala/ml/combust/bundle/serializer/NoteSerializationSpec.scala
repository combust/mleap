package ml.combust.bundle.serializer

import java.net.URI

import ml.combust.bundle.{BundleFile, BundleRegistry, TestUtil}
import org.scalatest.funspec.AnyFunSpec
import scala.util.Using

/**
  * Created by hollinwilkins on 12/24/16.
  */
class NoteSerializationSpec extends org.scalatest.funspec.AnyFunSpec {
  implicit val registry = BundleRegistry("test-registry")

  describe("Bundle.ML notes serialization") {
    it("serializes and deserializes text notes") {
      val uri = new URI(s"jar:file:${TestUtil.baseDir}/notes.bundle.zip")
      Using(BundleFile(uri)) { serializer =>
        serializer.writeNote("note.txt", "Another note")
        serializer.writeNote("test.txt", "Hello, there!")

        assert(serializer.readNote("note.txt") == "Another note")
        assert(serializer.readNote("test.txt") == "Hello, there!")
        assert(serializer.listNotes().toSet == Set("note.txt", "test.txt"))
      }
    }
  }
}
