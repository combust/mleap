package ml.combust.mleap.core.classification

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.apache.spark.ml.linalg.{Matrices, Vectors}
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hollinwilkins on 5/23/17.
  */
class LogisticRegressionModelSpec extends org.scalatest.funspec.AnyFunSpec {
  describe("BinaryLogisticRegression") {
    val weights = Vectors.dense(1.0, 2.0, 4.0)
    val intercept = 0.7

    describe("issue210: Logistic function not being applied") {
      val lr = BinaryLogisticRegressionModel(weights, intercept, 0.4)
      it("applies the logistic function for prediction") {
        val features = Vectors.dense(-1.0, 1.0, -0.5)

        assert(lr.predict(features) == 1.0)
      }
    }

    describe("issue386:Wrong Binary LogisticRegression predictions") {
      val lr = BinaryLogisticRegressionModel(weights, intercept, 0.4)
      it("compare binary logisticRegression prediction with the transform api predictions") {
        val features = Vectors.dense(-1.0, 1.0, -0.5)
        assert(lr.predict(features) == lr.probabilityToPrediction(lr.rawToProbability(lr.predictRaw(features))))
        assert(lr.predict(features) == 1.0)
      }

      it("compare binary logisticRegression prediction with rawToPrediction() results") {
        val features = Vectors.dense(-1.0, 1.0, -0.5)
        assert(lr.predict(features) == lr.rawToPrediction(lr.predictRaw(features)))
        assert(lr.predict(features) == 1.0)
      }
    }

    describe("issue386:Binary LogisticRegression predictions with 1.0 threshold"){
      val lr = BinaryLogisticRegressionModel(weights, intercept, 1.0)
      it("binary logisticRegression prediction equals zero for 1.0 threshold") {
        val features = Vectors.dense(-1.0, 1.0, -0.5)
        assert(lr.predict(features) == lr.probabilityToPrediction(lr.rawToProbability(lr.predictRaw(features))))
        assert(lr.predict(features) == 0.0)
      }
    }

    describe("issue386:Binary LogisticRegression predictions with 0.0 threshold"){
      val lr = BinaryLogisticRegressionModel(weights, intercept, 0.0)
      it("binary logisticRegression prediction equals 1 for zero threshold") {
        val features = Vectors.dense(-1.0, 1.0, -0.5)
        assert(lr.predict(features) == lr.rawToPrediction(lr.predictRaw(features)))
        assert(lr.predict(features) == 1.0)
      }
    }

    describe("input/output schema"){
      val lr = BinaryLogisticRegressionModel(weights, intercept, 0.4)
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
