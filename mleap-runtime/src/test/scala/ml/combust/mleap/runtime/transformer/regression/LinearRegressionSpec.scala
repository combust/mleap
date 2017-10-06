package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/15/16.
  */
class LinearRegressionSpec extends FunSpec {
  val schema = StructType(Seq(StructField("features", TensorType(BasicType.Double)))).get
  val dataset = Seq(Row(Tensor.denseVector(Array(20.0, 10.0, 5.0))))
  val frame = DefaultLeapFrame(schema, dataset)
  val linearRegression = LinearRegression(shape = NodeShape.regression(),
    model = LinearRegressionModel(coefficients = Vectors.dense(Array(1.0, 0.5, 5.0)),
      intercept = 73.0))

  describe("LinearRegression") {
    describe("#transform") {
      it("executes the linear regression model and outputs a prediction") {
        val frame2 = linearRegression.transform(frame).get
        val prediction = frame2.dataset(0).getDouble(1)

        assert(prediction == 123.0)
      }

      describe("with invalid features input") {
        it("returns a Failure") {
          val frame2 = linearRegression.copy(shape = NodeShape.regression(featuresCol = "bad_features")).transform(frame)

          assert(frame2.isFailure)
        }
      }
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(linearRegression.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}
