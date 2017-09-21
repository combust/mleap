package ml.combust.mleap.core.regression

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.BLAS

/**
  * Created by hollinwilkins on 12/28/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/regression/AFTSurvivalRegression.scala")
case class AFTSurvivalRegressionModel(coefficients: Vector,
                                      intercept: Double,
                                      quantileProbabilities: Array[Double],
                                      scale: Double) extends Model {
  def apply(features: Vector): Double = predict(features)

  def predictWithQuantiles(features: Vector): (Double, Vector) = {
    val quantiles = predictQuantiles(features)
    (predict(features), quantiles)
  }

  def predictQuantiles(features: Vector): Vector = {
    // scale parameter for the Weibull distribution of lifetime
    val lambda = math.exp(BLAS.dot(coefficients, features) + intercept)
    // shape parameter for the Weibull distribution of lifetime
    val k = 1 / scale
    val quantiles = quantileProbabilities.map {
      q => lambda * math.exp(math.log(-math.log(1 - q)) / k)
    }

    Vectors.dense(quantiles)
  }

  def predict(features: Vector): Double = {
    math.exp(BLAS.dot(coefficients, features) + intercept)
  }

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(coefficients.size)).get

  override def outputSchema: StructType = {
    StructType("prediction" -> ScalarType.Double.nonNullable,
      "quantiles" -> TensorType.Double(quantileProbabilities.length)).get
  }
}
