package ml.combust.mleap.core.regression

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 12/27/16.
  */
class IsotonicRegressionModelSpec extends FunSpec {
  val regression = IsotonicRegressionModel(boundaries = Array(0.0, 4.0, 5.0, 7.0, 8.0),
    predictions = Seq(100.0, 200.0, 300.0, 400.0, 500.0),
    isotonic = true,
    featureIndex = Some(2))

  describe("#apply") {
    it("applies the linear regression to a feature vector") {

      assert(regression(4.0) == 200.0)
      assert(regression(4.5) == 250.0)
      assert(regression(Vectors.dense(Array(1.0, 2.3, 7.2))) == 420.0)
    }
  }

  describe("input/output schema") {
    it("has the right input schema") {
      assert(regression.inputSchema.fields == Seq(StructField("features", TensorType.Double())))
    }

    it("has the right output schema") {
      assert(regression.outputSchema.fields == Seq(StructField("prediction", ScalarType.Double)))
    }
  }
}
