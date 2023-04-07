package ml.combust.mleap.runtime.transformer.regression

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree.{ContinuousSplit, InternalNode, LeafNode}
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.test.TestUtil
import org.scalatest.FunSpec
import resource.managed

import java.io.File
import java.net.URI

class DecisionTreeRegressionSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val node = InternalNode(LeafNode(Seq(0.78)), LeafNode(Seq(0.34)), ContinuousSplit(0, 0.5))
      val regression = DecisionTreeRegressionModel(node, 3)

      val transformer = DecisionTreeRegression(shape = NodeShape.regression(),
        model = regression)

      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }

  describe("save/load model") {
    it("correctly reproduces the model when saved and loaded") {
      val node = InternalNode(LeafNode(Seq(0.78)), LeafNode(Seq(0.34)), ContinuousSplit(0, 0.5))
      val regression = DecisionTreeRegressionModel(node, 3)

      val transformer = DecisionTreeRegression(shape = NodeShape.regression(),
        model = regression)

      // serialization
      val fileName = s"${TestUtil.baseDir}/decision_tree_regression_saved_model.json.zip"
      val uri = new URI(s"jar:file:$fileName")
      for (file <- managed(BundleFile(uri))) {
        transformer.writeBundle.name("bundle")
          .format(SerializationFormat.Json)
          .save(file)
      }

      // de-serialization
      val file = new File(fileName)
      val loadedTransformer = (for (bf <- managed(BundleFile(file))) yield {
        bf.loadMleapBundle().get.root
      }).tried.get.asInstanceOf[DecisionTreeRegression]

      // checks
      assert(transformer.inputSchema.fields equals (loadedTransformer.inputSchema.fields))
      assert(transformer.outputSchema.fields equals (loadedTransformer.outputSchema.fields))
    }
  }
}