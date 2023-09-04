package ml.combust.mleap.core.regression

import ml.combust.mleap.core.types.{ScalarShape, ScalarType, StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funspec.AnyFunSpec

class GeneralizedLinearRegressionModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("generalized linear regression model") {
    val model = new GeneralizedLinearRegressionModel(Vectors.dense(1, 2, 3), 23, null)

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features",TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("prediction", ScalarType.Double.nonNullable),
           StructField("link_prediction", ScalarType.Double.nonNullable)))
    }
  }
}
