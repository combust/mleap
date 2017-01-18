package ml.combust.bundle.serializer

import java.net.URI

import ml.combust.bundle.{BundleFile, BundleRegistry, TestUtil}
import org.scalatest.FunSpec
import resource._

/**
  * Created by hollinwilkins on 12/24/16.
  */
class NoteSerializationSpec extends FunSpec {
  implicit val registry = BundleRegistry("test-registry")

  describe("Bundle.ML notes serialization") {
    it("serializes and deserializes text notes") {
      val uri = new URI(s"jar:file:${TestUtil.baseDir}/notes.bundle.zip")
      for(serializer <- managed(BundleFile(uri))) {
        serializer.writeNote("note.txt", "Another note")
        serializer.writeNote("test.txt", "Hello, there!")

        assert(serializer.readNote("note.txt") == "Another note")
        assert(serializer.readNote("test.txt") == "Hello, there!")
        assert(serializer.listNotes().toSet == Set("note.txt", "test.txt"))
      }
    }
  }
}
