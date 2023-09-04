package ml.combust.mleap.core.classification

import ml.combust.mleap.core.test.TestUtil
import ml.combust.mleap.core.types.{BasicType, ScalarType, StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hollinwilkins on 9/28/16.
  */
class GBTClassifierModelSpec extends org.scalatest.funspec.AnyFunSpec {
  val tree1 = TestUtil.buildDecisionTreeRegression(0.5, 0, goLeft = true)
  val tree2 = TestUtil.buildDecisionTreeRegression(0.75, 1, goLeft = false)
  val tree3 = TestUtil.buildDecisionTreeRegression(-0.1, 2, goLeft = true)

  val classifier = GBTClassifierModel(trees = Seq(tree1, tree2, tree3),
    treeWeights = Seq(0.5, 2.0, 1.0),
    numFeatures = 3)

  describe("#apply") {
    val features = Vectors.dense(Array(0.2, 0.8, 0.4))

    it("predicts the class based on the features") {
      assert(classifier(features) == 1.0)
    }
  }

  describe("input/output schema") {
    it("has the right input schema") {
      assert(classifier.inputSchema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3)))))
    }

    it("has the right output schema") {
      assert(classifier.outputSchema.fields ==
        Seq(StructField("raw_prediction", TensorType.Double(2)),
          StructField("probability", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double.nonNullable)
        ))
    }
  }
}
