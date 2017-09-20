package ml.combust.mleap.core.classification

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.apache.spark.ml.linalg.{Matrices, Vectors}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 5/23/17.
  */
class LogisticRegressionModelSpec extends FunSpec {
  describe("BinaryLogisticRegression") {
    val weights = Vectors.dense(1.0, 2.0, 4.0)
    val intercept = 0.7
    val lr = BinaryLogisticRegressionModel(weights, intercept, 0.4)

    describe("issue210: Logistic function not being applied") {
      it("applies the logistic function for prediction") {
        val features = Vectors.dense(-1.0, 1.0, -0.5)

        assert(lr.predict(features) == 1.0)
      }
    }

    describe("input/output schema"){
      it("has the right input schema") {
        assert(lr.inputSchema.fields == Seq(StructField("features", TensorType.Double(3))))
      }

      it("has the right output schema") {
        assert(lr.outputSchema.fields == Seq(
          StructField("raw_prediction", TensorType.Double(2)),
          StructField("probability", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double.nonNullable)
        ))
      }
    }
  }

  describe("ProbabilisticLogisticsRegressionModel") {
    val weights = Matrices.dense(3, 3, Array(1, 2, 3, 1, 2, 3, 1, 2, 3))
    val intercept = Vectors.dense(1, 2, 3)
    val lr = ProbabilisticLogisticsRegressionModel(weights, intercept, None)

    describe("input/output schema"){
      it("has the right input schema") {
        assert(lr.inputSchema.fields == Seq(StructField("features", TensorType.Double(3))))
      }

      it("has the right output schema") {
        assert(lr.outputSchema.fields == Seq(
          StructField("raw_prediction", TensorType.Double(3)),
          StructField("probability", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double.nonNullable)
        ))
      }
    }
  }
}
