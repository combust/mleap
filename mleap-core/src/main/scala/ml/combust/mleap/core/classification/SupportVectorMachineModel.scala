package ml.combust.mleap.core.classification

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.BLAS

/** Companion object for holding constants.
  */
object SupportVectorMachineModel {
  val defaultThresholds = Array(0.5, 0.5)
}

/** Class for support vector machine models.
  *
  * @param coefficients coefficients of SVM
  * @param intercept intercept for SVM
  * @param thresholds threshold for pegging prediction
  */
case class SupportVectorMachineModel(coefficients: Vector,
                                     intercept: Double,
                                     override val thresholds: Option[Array[Double]] = Some(SupportVectorMachineModel.defaultThresholds))
  extends ProbabilisticClassificationModel with Serializable {
  private def margin(features: Vector): Double = BLAS.dot(coefficients, features) + intercept

  override val numClasses: Int = 2

  override def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(Array(-m, m))
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = raw
}
