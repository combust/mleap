package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree.{ContinuousSplit, InternalNode, LeafNode}
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

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
}