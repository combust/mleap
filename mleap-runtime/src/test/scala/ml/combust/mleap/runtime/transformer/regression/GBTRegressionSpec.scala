package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.GBTRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.test.TestUtil
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.Tensor
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hollinwilkins on 9/28/16.
  */
class GBTRegressionSpec extends org.scalatest.funspec.AnyFunSpec {
  val schema = StructType(Seq(StructField("features", TensorType(BasicType.Double)))).get
  val dataset = Seq(Row(Tensor.denseVector(Array(0.2, 0.7, 0.4))))
  val frame = DefaultLeapFrame(schema, dataset)
  val tree1 = TestUtil.buildDecisionTreeRegression(0.5, 0, goLeft = true)
  val tree2 = TestUtil.buildDecisionTreeRegression(0.75, 1, goLeft = false)
  val tree3 = TestUtil.buildDecisionTreeRegression(0.1, 2, goLeft = true)
  val gbt = GBTRegression(shape = NodeShape.regression(),
    model = GBTRegressionModel(Seq(tree1, tree2, tree3), Seq(0.5, 2.0, 1.0), 5))

  describe("#transform") {
    it("uses the GBT to make predictions on the features column") {
      val frame2 = gbt.transform(frame).get
      val prediction = frame2.dataset(0).getDouble(1)

      assert(prediction == (0.5 * 0.5 + 0.75 * 2.0 + 0.1 * 1.0))
    }

    describe("with invalid features column") {
      val gbt2 = gbt.copy(shape = NodeShape.regression(featuresCol = "bad_features"))

      it("returns a Failure") { assert(gbt2.transform(frame).isFailure) }
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(gbt.schema.fields ==
        Seq(StructField("features", TensorType.Double(5)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}
