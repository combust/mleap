package ml.combust.mleap.core.regression

import ml.combust.mleap.core.tree.{ContinuousSplit, InternalNode, LeafNode}
import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class DecisionTreeRegressionModelSpec extends FunSpec {
  val node = InternalNode(LeafNode(Seq(0.78)), LeafNode(Seq(0.34)), ContinuousSplit(0, 0.5))
  val regression = DecisionTreeRegressionModel(node, 5)

  describe("#predict") {
    it("returns the prediction for the decision tree") {
      val features = Vectors.dense(Array(0.3, 1.0, 43.23, -21.2, 66.7))

      assert(regression.predict(features) == 0.78)
    }
  }

  describe("input/output schema") {
    it("has the right input schema") {
      assert(regression.inputSchema.fields == Seq(StructField("features", TensorType.Double(5))))
    }

    it("has the right output schema") {
      assert(regression.outputSchema.fields == Seq(StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}
