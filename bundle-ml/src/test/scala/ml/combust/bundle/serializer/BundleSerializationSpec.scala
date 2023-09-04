package ml.combust.bundle.serializer

import java.net.URI
import ml.bundle.Attributes
import ml.combust.bundle.test.TestSupport._
import ml.combust.bundle.test._
import ml.combust.bundle.test.ops._
import ml.combust.bundle.{BundleFile, BundleRegistry, TestUtil, dsl}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Using
import scala.util.Random

/**
  * Created by hollinwilkins on 8/21/16.
  */
sealed trait FSType
case object ZipFS extends FSType
case object DirFS extends FSType
case object DirInJarFS extends FSType

class BundleSerializationSpec extends AnyFunSpec with Matchers {
  implicit val testContext = TestContext(BundleRegistry("test-registry"))

  it should behave like bundleSerializer("Serializing/Deserializing json a bundle as a dir",
    SerializationFormat.Json,
    DirFS)
  it should behave like bundleSerializer("Serializing/Deserializing json a bundle as a zip",
    SerializationFormat.Json,
    ZipFS)
  it should behave like bundleSerializer("Serializing/Deserializing json a bundle as a dir in zip",
    SerializationFormat.Json,
    DirInJarFS)

  it should behave like bundleSerializer("Serializing/Deserializing proto a bundle as a dir",
    SerializationFormat.Protobuf,
    DirFS)
  it should behave like bundleSerializer("Serializing/Deserializing proto a bundle as a zip",
    SerializationFormat.Protobuf,
    DirFS)
  it should behave like bundleSerializer("Serializing/Deserializing proto a bundle as a dir in zip",
    SerializationFormat.Protobuf,
    DirInJarFS)

  def bundleSerializer(description: String, format: SerializationFormat, fsType: FSType) = {
    val (prefix, suffix) = fsType match {
      case DirFS => ("file", "")
      case ZipFS => ("jar:file", ".zip")
      case DirInJarFS => ("jar:file", ".jar!custom/path")
    }

    describe(description) {
      val randomCoefficients = (0 to 100000).map(v => Random.nextDouble())
      val lr = LinearRegression(uid = "linear_regression_example",
        input = "input_field",
        output = "output_field",
        model = LinearModel(coefficients = randomCoefficients,
          intercept = 44.5))
      val si = StringIndexer(uid = "string_indexer_example",
        input = "input_string",
        output = "output_index",
        model = StringIndexerModel(strings = Seq("hey", "there", "man")))
      val pipeline = Pipeline(uid = "my_pipeline", PipelineModel(Seq(si, lr)))

      describe("with a simple linear regression") {
        it("serializes/deserializes the same object") {
          val uri = new URI(s"$prefix:${TestUtil.baseDir}/lr_bundle.$format$suffix")
          Using(BundleFile(uri)) {file =>
            lr.writeBundle.name("my_bundle").
              format(format).
              save(file)

            file.loadBundle()


          }.flatten.get.root shouldBe lr
        }
      }

      describe("with a decision tree") {
        it("serializes/deserializes the same object") {
          val node = InternalNode(CategoricalSplit(1, isLeft = true, 5, Seq(1.0, 3.0)),
            InternalNode(ContinuousSplit(2, 0.4), LeafNode(Seq(5.0)), LeafNode(Seq(4.0))),
            LeafNode(Seq(0.4, 5.6, 3.2, 5.7, 5.5)))
          val dt = DecisionTreeRegression(uid = "my_decision_tree",
            input = "my_input",
            output = "my_output",
            model = DecisionTreeRegressionModel(node))

          val uri = new URI(s"$prefix:${TestUtil.baseDir}/dt_bundle.$format$suffix")
          Using(BundleFile(uri)) { file =>
            dt.writeBundle.name("my_bundle").
              format(format).
              save(file)

            file.loadBundle()
          }.flatten.get.root shouldBe dt
        }
      }

      describe("with a pipeline") {
        it("serializes/deserializes the same object") {
          val uri = new URI(s"$prefix:${TestUtil.baseDir}/pipeline_bundle.$format$suffix")
          Using(BundleFile(uri)) { file =>
            pipeline.writeBundle.name("my_bundle").
              format(format).
              save(file)

            file.loadBundle()
          }.flatten.get.root shouldBe pipeline
        }
      }

      describe("with a backslash'd path for a linear regression") {
        it("serializes and deserializes from a path containing '\\' separators") {
          val uri = s"$prefix:${TestUtil.baseDir}/lr_bundle.$format$suffix".replace('/', '\\')
          Using(BundleFile(uri)) { file =>
            lr.writeBundle.name("my_bundle").
              format(format).
              save(file)

            file.loadBundle()
          }.flatten.get.root shouldBe lr
        }
      }

      describe("with metadata") {
        it("serializes and deserializes any metadata we want to send along with the bundle") {
          val uri = new URI(s"$prefix:${TestUtil.baseDir}/lr_bundle_meta.$format$suffix")
          val meta = new Attributes().withList(Map(
            "keyA" -> dsl.Value.string("valA").value,
            "keyB" -> dsl.Value.double(1.2).value,
            "keyC" -> dsl.Value.stringList(Seq("listValA", "listValB")).value
          ))
          val bundleReadMeta = Using(BundleFile(uri)) { file =>
            lr.writeBundle.name("my_bundle").
              format(format).
              meta(meta).
              save(file)

            file.loadBundle()
          }.flatten.get.info.meta
          bundleReadMeta shouldBe defined
          bundleReadMeta shouldBe Some(meta)
        }
      }
    }
  }
}
