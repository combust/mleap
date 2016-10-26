package ml.combust.bundle.serializer

import java.io.File

import ml.combust.bundle.{BundleRegistry, TestUtil}
import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.test_ops._
import org.scalatest.FunSpec

import scala.util.Random

/**
  * Created by hollinwilkins on 8/21/16.
  */
class BundleSerializationSpec extends FunSpec {
  implicit val registry = BundleRegistry("test")

  it should behave like bundleSerializer("Serializing/Deserializing mixed a bundle as a dir",
    SerializationFormat.Mixed,
    "")
  it should behave like bundleSerializer("Serializing/Deserializing mixed a bundle as a zip",
    SerializationFormat.Mixed,
    ".zip")

  it should behave like bundleSerializer("Serializing/Deserializing json a bundle as a dir",
    SerializationFormat.Json,
    "")
  it should behave like bundleSerializer("Serializing/Deserializing json a bundle as a zip",
    SerializationFormat.Json,
    ".zip")

  it should behave like bundleSerializer("Serializing/Deserializing proto a bundle as a dir",
    SerializationFormat.Protobuf,
    "")
  it should behave like bundleSerializer("Serializing/Deserializing proto a bundle as a zip",
    SerializationFormat.Protobuf,
    ".zip")

  def bundleSerializer(description: String, format: SerializationFormat, suffix: String) = {
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
          val file = new File(TestUtil.baseDir, s"lr_bundle.$format$suffix")
          val bundle = Bundle.createBundle("my_bundle", format, Seq(lr))
          val serializer = BundleSerializer(Unit, file)
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

          val file = new File(TestUtil.baseDir, s"decision_tree_bundle.$format$suffix")
          val bundle = Bundle.createBundle("my_bundle", format, Seq(dt))
          val serializer = BundleSerializer(Unit, file)
          serializer.write(bundle)
          val bundleRead = serializer.read()

          assert(dt == bundleRead.nodes.head)
        }
      }

      describe("with a pipeline") {
        it("serializes/deserializes the same object") {
          val file = new File(TestUtil.baseDir, s"pipeline_bundle.$format$suffix")
          val bundle = Bundle.createBundle("my_bundle", format, Seq(pipeline))
          val serializer = BundleSerializer(Unit, file)
          serializer.write(bundle)
          val bundleRead = serializer.read()

          assert(pipeline == bundleRead.nodes.head)
        }
      }
    }
  }
}
