package ml.combust.mleap.core.classification

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
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

  override def predictBinaryProbability(features: Vector): Double = {
    1.0 / (1.0 + math.exp(-margin(features)))
  }

  override def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    raw match {
      case dv: DenseVector =>
        var i = 0
        val size = dv.size
        while (i < size) {
          dv.values(i) = 1.0 / (1.0 + math.exp(-dv.values(i)))
          i += 1
        }
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in LogisticRegressionModel:" +
          " raw2probabilitiesInPlace encountered SparseVector")
    }
  }
}
