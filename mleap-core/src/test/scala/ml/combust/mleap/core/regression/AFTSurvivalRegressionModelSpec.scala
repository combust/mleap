package ml.combust.mleap.core.regression

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funspec.AnyFunSpec

class AFTSurvivalRegressionModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("AFT survival regression model") {
    val model = new AFTSurvivalRegressionModel(Vectors.dense(1, 2, 3), 2, Array(4, 5, 6, 7), 3)

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features",TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("prediction", ScalarType.Double.nonNullable),
          StructField("quantiles", TensorType.Double(4))))
    }
  }
}
