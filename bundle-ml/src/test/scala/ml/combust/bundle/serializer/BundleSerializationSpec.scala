package ml.combust.bundle.serializer

import java.io.File
import java.net.URI

import ml.combust.bundle.{BundleRegistry, TestUtil}
import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.test._
import ml.combust.bundle.test.ops._
import org.scalatest.FunSpec

import scala.util.Random

/**
  * Created by hollinwilkins on 8/21/16.
  */
sealed trait FSType
case object ZipFS extends FSType
case object DirFS extends FSType

class BundleSerializationSpec extends FunSpec {
  implicit val registry = BundleRegistry("test")

  it should behave like bundleSerializer("Serializing/Deserializing mixed a bundle as a dir",
    SerializationFormat.Mixed,
    DirFS)
  it should behave like bundleSerializer("Serializing/Deserializing mixed a bundle as a zip",
    SerializationFormat.Mixed,
    ZipFS)

  it should behave like bundleSerializer("Serializing/Deserializing json a bundle as a dir",
    SerializationFormat.Json,
    DirFS)
  it should behave like bundleSerializer("Serializing/Deserializing json a bundle as a zip",
    SerializationFormat.Json,
    ZipFS)

  it should behave like bundleSerializer("Serializing/Deserializing proto a bundle as a dir",
    SerializationFormat.Protobuf,
    DirFS)
  it should behave like bundleSerializer("Serializing/Deserializing proto a bundle as a zip",
    SerializationFormat.Protobuf,
    DirFS)

  def bundleSerializer(description: String, format: SerializationFormat, fsType: FSType) = {
    val (prefix, suffix) = fsType match {
      case DirFS => ("file", "")
      case ZipFS => ("jar:file", ".zip")
    }

    describe(description) {
      val randomCoefficients = (0 to 100000).map(v => Random.nextDouble())
      val lr = LinearRegression(uid = "linear_regression_example",
        input = "input_field",
        output = "output_field",
        model = LinearModel(coefficients = randomCoefficients,
          intercept = 44.5))
      val custom = MyCustomTransformer(MyCustomObject("some_custom"))
      val si = StringIndexer(uid = "string_indexer_example",
        input = "input_string",
        output = "output_index",
        model = StringIndexerModel(strings = Seq("hey", "there", "man")))
      val pipeline = Pipeline(uid = "my_pipeline", PipelineModel(Seq(si, custom, lr)))

      describe("with a simple linear regression") {
        it("serializes/deserializes the same object") {
          val uri = new URI(s"$prefix:${TestUtil.baseDir}/lr_bundle.$format$suffix")
          val bundle = Bundle.createBundle("my_bundle", format, Seq(lr))
          val serializer = BundleSerializer(Unit, uri)
          serializer.write(bundle)
          val bundleRead = serializer.read()

          assert(lr == bundleRead.nodes.head)
        }
      }

      describe("with a decision tree") {
        it("serializes/deserializes the same object") {
          val node = InternalNode(CategoricalSplit(1, isLeft = true, 5, Seq(1.0, 3.0)),
            InternalNode(ContinuousSplit(2, 0.4), LeafNode(5.0, Some(Seq())), LeafNode(4.0, Some(Seq()))),
            LeafNode(3.0, Some(Seq(0.4, 5.6, 3.2, 5.7, 5.5))))
          val dt = DecisionTreeRegression(uid = "my_decision_tree",
            input = "my_input",
            output = "my_output",
            model = DecisionTreeRegressionModel(node))

          val uri = new URI(s"$prefix:${TestUtil.baseDir}/lr_bundle.$format$suffix")
          val bundle = Bundle.createBundle("my_bundle", format, Seq(dt))
          val serializer = BundleSerializer(Unit, uri)
          serializer.write(bundle)
          val bundleRead = serializer.read()

          assert(dt == bundleRead.nodes.head)
        }
      }

      describe("with a pipeline") {
        it("serializes/deserializes the same object") {
          val uri = new URI(s"$prefix:${TestUtil.baseDir}/pipeline_bundle.$format$suffix")
          val bundle = Bundle.createBundle("my_bundle", format, Seq(pipeline))
          val serializer = BundleSerializer(Unit, uri)
          serializer.write(bundle)
          val bundleRead = serializer.read()

          assert(pipeline == bundleRead.nodes.head)
        }
      }
    }
  }
}
