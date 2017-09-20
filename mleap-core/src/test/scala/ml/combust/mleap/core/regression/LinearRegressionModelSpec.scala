package ml.combust.mleap.core.regression

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.scalatest.FunSpec
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hwilkins on 1/21/16.
  */
class LinearRegressionModelSpec extends FunSpec {
  val linearRegression = LinearRegressionModel(Vectors.dense(Array(0.5, 0.75, 0.25)), .33)

  describe("#apply") {
    it("applies the linear regression to a feature vector") {
      assert(linearRegression(Vectors.dense(Array(1.0, 0.5, 1.0))) == 1.455)
    }
  }

  describe("input/output schema") {
    it("has the right input schema") {
      assert(linearRegression.inputSchema.fields == Seq(StructField("features", TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(linearRegression.outputSchema.fields == Seq(StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}
