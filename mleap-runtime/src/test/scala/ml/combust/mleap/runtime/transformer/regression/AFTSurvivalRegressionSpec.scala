package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.AFTSurvivalRegressionModel
import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class AFTSurvivalRegressionSpec extends FunSpec {

  describe("input/output schema") {

    it("has the correct inputs and outputs") {
      val transformer = AFTSurvivalRegression(shape = NodeShape.regression()
        .withOutput("quantiles", "quantiles"),
        model = new AFTSurvivalRegressionModel(Vectors.dense(1, 3, 4), 23, Array(1, 2, 3, 4, 5), 5))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double.nonNullable),
          StructField("quantiles", TensorType.Double(5))))
    }
  }
}
