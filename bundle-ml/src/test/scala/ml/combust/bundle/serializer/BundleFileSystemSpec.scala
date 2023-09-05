package ml.combust.bundle.serializer

import java.net.URI
import java.nio.file.Files

import ml.combust.bundle.test.TestSupport._
import ml.combust.bundle.{BundleFile, BundleRegistry}
import ml.combust.bundle.test.ops._
import ml.combust.bundle.test.{TestBundleFileSystem, TestContext}
import org.scalatest.funspec.AnyFunSpec
import scala.util.Using

import scala.util.Random

class BundleFileSystemSpec extends org.scalatest.funspec.AnyFunSpec {
  implicit val testContext = TestContext(BundleRegistry("test-registry").
    registerFileSystem(new TestBundleFileSystem))

  val randomCoefficients = (0 to 100000).map(v => Random.nextDouble())
  val lr = LinearRegression(uid = "linear_regression_example",
    input = "input_field",
    output = "output_field",
    model = LinearModel(coefficients = randomCoefficients,
      intercept = 44.5))

  describe("saving/loading bundle file using test file system") {
    it("loads/saves using the custom file system") {
      val tmpDir = Files.createTempDirectory("BundleFileSystemSpec")
      val uri = new URI(s"test://$tmpDir/test.zip")

      lr.writeBundle.name("my_bundle").save(uri)
      val loaded = uri.loadBundle().get

      assert(loaded.root == lr)
    }
  }
}
