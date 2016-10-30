package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.LogisticRegressionModel
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.runtime.types.{StructField, StructType, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/15/16.
  */
class LogisticRegressionSpec extends FunSpec {
  val schema = StructType(Seq(StructField("features", TensorType.doubleVector()))).get
  val dataset = LocalDataset(Array(Row(Vectors.dense(Array(0.5, -0.5, 1.0)))))
  val frame = LeapFrame(schema, dataset)
  val logisticRegression = LogisticRegression(featuresCol = "features",
    predictionCol = "prediction",
    model = LogisticRegressionModel(coefficients = Vectors.dense(Array(1.0, 1.0, 2.0)),
      intercept = -0.2,
      threshold = Some(0.75)))

  describe("LogisticRegression") {
    describe("#transform") {
      it("executes the logistic regression model and outputs the prediction") {
        val frame2 = logisticRegression.transform(frame).get
        val prediction = frame2.dataset.toArray(0).getDouble(1)

        assert(prediction == 1.0)
      }

      describe("with probability column") {
        val logisticRegression2 = logisticRegression.copy(probabilityCol = Some("probability"))

        it("executes the logistic regression model and outputs the prediction/probability") {
          val frame2 = logisticRegression2.transform(frame).get
          val probability = frame2.dataset.toArray(0).getDouble(1)
          val prediction = frame2.dataset.toArray(0).getDouble(2)

          assert(prediction == 1.0)
          assert(probability > 0.84)
          assert(probability < 0.86)
        }
      }

      describe("with invalid features column") {
        val logisticRegression2 = logisticRegression.copy(featuresCol = "bad_features")

        it("returns a Failure") { assert(logisticRegression2.transform(frame).isFailure) }
      }
    }
  }
}
