package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.{BinaryLogisticRegressionModel, LogisticRegressionModel}
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/15/16.
  */
class LogisticRegressionSpec extends FunSpec {
  val schema = StructType(Seq(StructField("features", TensorType(BasicType.Double)))).get
  val dataset = LocalDataset(Seq(Row(Tensor.denseVector(Array(0.5, -0.5, 1.0)))))
  val frame = LeapFrame(schema, dataset)
  val logisticRegression = LogisticRegression(shape = NodeShape.probabilisticClassifier(3, 2),
    model = LogisticRegressionModel(BinaryLogisticRegressionModel(coefficients = Vectors.dense(Array(1.0, 1.0, 2.0)),
      intercept = -0.2,
      threshold = 0.75)))

  describe("LogisticRegression") {
    describe("#transform") {
      it("executes the logistic regression model and outputs the prediction") {
        val frame2 = logisticRegression.transform(frame).get
        val prediction = frame2.dataset(0).getDouble(1)

        assert(prediction == 1.0)
      }

      describe("with probability column") {
        val logisticRegression2 = logisticRegression.copy(shape = NodeShape.probabilisticClassifier(3, 2, probabilityCol = Some("probability")))

        it("executes the logistic regression model and outputs the prediction/probability") {
          val frame2 = logisticRegression2.transform(frame).get
          val data = frame2.dataset.toArray
          val probability = data(0).getTensor[Double](1)(1)
          val prediction = data(0).getDouble(2)

          assert(prediction == 1.0)
          assert(probability > 0.84)
          assert(probability < 0.86)
        }
      }

      describe("with invalid features column") {
        val logisticRegression2 = logisticRegression.copy(shape = NodeShape.probabilisticClassifier(3, 2, featuresCol = "bad_features"))

        it("returns a Failure") { assert(logisticRegression2.transform(frame).isFailure) }
      }
    }

    describe("#getFields") {
      it("has the correct inputs and outputs") {
        assert(logisticRegression.schema.fields ==
          Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
            StructField("prediction", ScalarType.Double)))
      }

      it("has the correct inputs and outputs with probability column") {
        val logisticRegression2 = logisticRegression.copy(shape = NodeShape.probabilisticClassifier(3, 2, probabilityCol = Some("probability")))
        assert(logisticRegression2.schema.fields ==
          Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
            StructField("probability", TensorType(BasicType.Double, Seq(2))),
            StructField("prediction", ScalarType.Double)))
      }

      it("has the correct inputs and outputs with rawPrediction column") {
        val logisticRegression2 = logisticRegression.copy(shape = NodeShape.probabilisticClassifier(3, 2, rawPredictionCol = Some("rp")))
        assert(logisticRegression2.schema.fields ==
          Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
            StructField("rp", TensorType(BasicType.Double, Seq(2))),
            StructField("prediction", ScalarType.Double)))
      }

      it("has the correct inputs and outputs with both probability and rawPrediction column") {
        val logisticRegression2 = logisticRegression.copy(shape = NodeShape.probabilisticClassifier(3, 2,
          rawPredictionCol = Some("rp"),
          probabilityCol = Some("p")))
        assert(logisticRegression2.schema.fields ==
          Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
            StructField("rp", TensorType(BasicType.Double, Seq(2))),
            StructField("p", TensorType(BasicType.Double, Seq(2))),
            StructField("prediction", ScalarType.Double)))
      }
    }
  }
}
