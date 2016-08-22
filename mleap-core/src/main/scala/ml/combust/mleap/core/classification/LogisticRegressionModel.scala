package ml.combust.mleap.core.classification

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.mleap.BLAS


/** Class for binary logistic regression models.
  *
  * @param coefficients coefficients vector for model
  * @param intercept intercept of model
  * @param threshold threshold for pegging predictions
  */
case class LogisticRegressionModel(coefficients: Vector,
                                   intercept: Double,
                                   override val threshold: Option[Double] = Some(0.5))
  extends BinaryClassificationModel
  with Serializable {
  /** Computes the mean of the response variable given the predictors. */
  private def margin(features: Vector): Double = {
    BLAS.dot(features, coefficients) + intercept
  }

  override def predictProbability(features: Vector): Double = {
    1.0 / (1.0 + math.exp(-margin(features)))
  }
}
