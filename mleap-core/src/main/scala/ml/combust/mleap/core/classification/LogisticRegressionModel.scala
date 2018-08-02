package ml.combust.mleap.core.classification

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.linalg.mleap.BLAS

trait AbstractLogisticRegressionModel extends ProbabilisticClassificationModel

case class BinaryLogisticRegressionModel(coefficients: Vector,
                                         intercept: Double,
                                         threshold: Double) extends AbstractLogisticRegressionModel {
  def margin(features: Vector): Double = {
    BLAS.dot(features, coefficients) + intercept
  }

  override def predict(features: Vector): Double = if(score(features) > threshold) 1.0 else 0.0

  override val numClasses: Int = 2
  override val numFeatures: Int = coefficients.size

  def score(features: Vector): Double = {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }

  override def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(Array(-m, m))
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = raw match {
    case dv: DenseVector =>
      var i = 0
      val size = dv.size
      while (i < size) {
        dv.values(i) = 1.0 / (1.0 + math.exp(-dv.values(i)))
        i += 1
      }
      dv
    case _ => throw new RuntimeException(s"unsupported vector type ${raw.getClass}")
  }

  override def probabilityToPrediction(probability: Vector): Double = {
    if (probability(1) > threshold) 1 else 0
  }

  override def rawToPrediction(rawPrediction: Vector): Double = {
    val rawThreshold = if (threshold == 0.0) {
      Double.NegativeInfinity
    } else if (threshold == 1.0) {
      Double.PositiveInfinity
    } else {
      math.log(threshold / (1.0 - threshold))
    }
    if (rawPrediction(1) > rawThreshold) 1 else 0
  }
}

case class ProbabilisticLogisticsRegressionModel(coefficientMatrix: Matrix,
                                                 interceptVector: Vector,
                                                 override val thresholds: Option[Array[Double]]) extends AbstractLogisticRegressionModel {
  override val numClasses: Int = interceptVector.size
  override val numFeatures: Int = coefficientMatrix.numCols

  private def margins(features: Vector): Vector = {
    val m = interceptVector.toDense.copy
    BLAS.gemv(1.0, coefficientMatrix, features, 1.0, m)
    m
  }

  override def predictRaw(features: Vector): Vector = {
    margins(features)
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    raw match {
      case dv: DenseVector =>
        val size = dv.size
        val values = dv.values

        // get the maximum margin
        val maxMarginIndex = raw.argmax
        val maxMargin = raw(maxMarginIndex)

        if(maxMargin == Double.PositiveInfinity) {
          var k = 0
          while(k < size) {
            values(k) = if (k == maxMarginIndex) 1.0 else 0.0
            k += 1
          }
        } else {
          val sum = {
            var temp = 0.0
            var k = 0
            while(k < numClasses) {
              values(k) = if (maxMargin > 0) {
                math.exp(values(k) - maxMargin)
              } else {
                math.exp(values(k))
              }
              temp += values(k)
              k += 1
            }
            temp
          }
          BLAS.scal(1 / sum, dv)
        }
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in LogisticRegressionModel:" +
          " raw2probabilitiesInPlace encountered SparseVector")
    }
  }
}

case class LogisticRegressionModel(impl: AbstractLogisticRegressionModel) extends ProbabilisticClassificationModel {
  override val numClasses: Int = impl.numClasses
  override val numFeatures: Int = impl.numFeatures
  val isMultinomial: Boolean = impl.numClasses > 2

  def multinomialModel: ProbabilisticLogisticsRegressionModel = impl.asInstanceOf[ProbabilisticLogisticsRegressionModel]
  def binaryModel: BinaryLogisticRegressionModel = impl.asInstanceOf[BinaryLogisticRegressionModel]

  override def predict(features: Vector): Double = impl.predict(features)

  override def predictRaw(features: Vector): Vector = impl.predictRaw(features)

  override def rawToProbabilityInPlace(raw: Vector): Vector = impl.rawToProbabilityInPlace(raw)

  override def probabilityToPrediction(probability: Vector): Double = impl.probabilityToPrediction(probability)

  override def rawToPrediction(raw: Vector): Double = impl.rawToPrediction(raw)
}
