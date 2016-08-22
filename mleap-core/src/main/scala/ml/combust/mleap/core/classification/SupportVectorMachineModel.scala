package ml.combust.mleap.core.classification

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.BLAS

/** Companion object for holding constants.
  */
object SupportVectorMachineModel {
  val defaultThreshold = 0.5
}

/** Class for support vector machine models.
  *
  * @param coefficients coefficients of SVM
  * @param intercept intercept for SVM
  * @param threshold threshold for pegging prediction
  */
case class SupportVectorMachineModel(coefficients: Vector,
                                     intercept: Double,
                                     override val threshold: Option[Double] = Some(SupportVectorMachineModel.defaultThreshold))
  extends BinaryClassificationModel with Serializable {
  private def margin(features: Vector): Double = BLAS.dot(coefficients, features) + intercept

  // TODO: this is not right, need to support multiple kernels
  // and actually do the SVM stuff properly
  override def predictProbability(features: Vector): Double = margin(features)
}
