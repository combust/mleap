package ml.combust.mleap.runtime

import java.net.URI
import java.nio.file.{Files, Paths}

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.transformer.feature.StringIndexer
import MleapSupport._

import org.scalatest.funspec.AnyFunSpec

class MleapSupportSpec extends org.scalatest.funspec.AnyFunSpec {
  private val testDir = Files.createTempDirectory("MleapSupportSpec")

  private val stringIndexer = StringIndexer(shape = NodeShape().
    withStandardInput("feature").
    withStandardOutput("feature_index"),
    model = StringIndexerModel(Seq(Seq("label1", "label2"))))

  describe("URIBundleFileOps") {
    it("can save/load a bundle using a URI") {
      val testFile = Paths.get(testDir.toString, "URIBundleFileOps.zip")
      testFile.toFile.deleteOnExit()

      val uri = new URI(s"jar:file://$testFile")
      stringIndexer.writeBundle.save(uri)

      val loadedStringIndexer = uri.loadMleapBundle().get.root

      assert(stringIndexer == loadedStringIndexer)
    }
  }
}
