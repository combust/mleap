package ml.bundle.serializer

import java.io.File
import java.util.UUID

import ml.bundle.TestUtil
import ml.bundle.dsl.Bundle
import ml.bundle.test_ops._
import org.scalatest.FunSpec

import scala.util.Random

/**
  * Created by hollinwilkins on 8/21/16.
  */
class BundleSerializationSpec extends FunSpec {
  val registry = new BundleRegistry().
    register(LinearRegressionOp).
    register(StringIndexerOp).
    register(PipelineOp).
    register(DecisionTreeRegressionOp)

  describe("Serializing/Deserializing a bundle") {
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
        val file = new File(TestUtil.baseDir, "lr_bundle")
        val bundle = Bundle.createBundle("my_bundle", SerializationFormat.Mixed, Seq(lr))
        val serializer = BundleSerializer(registry, file)
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

        val file = new File(TestUtil.baseDir, "decision_tree_bundle")
        val bundle = Bundle.createBundle("my_bundle", SerializationFormat.Mixed, Seq(dt))
        val serializer = BundleSerializer(registry, file)
        serializer.write(bundle)
        val bundleRead = serializer.read()

        assert(dt == bundleRead.nodes.head)
      }
    }

    describe("with a pipeline") {
      it("serializes/deserializes the same object") {
        val file = new File(TestUtil.baseDir, "pipeline_bundle")
        val bundle = Bundle.createBundle("my_bundle", SerializationFormat.Mixed, Seq(pipeline))
        val serializer = BundleSerializer(registry, file)
        serializer.write(bundle)
        val bundleRead = serializer.read()

        assert(pipeline == bundleRead.nodes.head)
      }
    }
  }
}
