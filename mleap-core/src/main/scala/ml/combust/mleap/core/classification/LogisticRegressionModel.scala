package ml.combust.mleap.core.classification

import breeze.linalg.{CSCMatrix, DenseMatrix, DenseVector, Matrix, SparseVector, Vector}
import breeze.optimize.proximal.QuadraticMinimizer.gemv
import ml.combust.mleap.core.linalg.BLAS

trait AbstractLogisticRegressionModel extends ProbabilisticClassificationModel

case class BinaryLogisticRegressionModel(coefficients: Vector[Double],
                                         intercept: Double,
                                         threshold: Double) extends AbstractLogisticRegressionModel {
  def margin(features: Vector[Double]): Double = {
    features.dot(coefficients) + intercept
  }

  override def predict(features: Vector[Double]): Double = if(score(features) > threshold) 1.0 else 0.0

  override val numClasses: Int = 2
  override val numFeatures: Int = coefficients.size

  def score(features: Vector[Double]): Double = {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }

  override def predictRaw(features: Vector[Double]): Vector[Double] = {
    val m = margin(features)
    DenseVector[Double](Array(-m, m))
  }

  override def rawToProbabilityInPlace(raw: Vector[Double]): Vector[Double] = raw match {
    case _dv: DenseVector[_] =>
      val dv = _dv.asInstanceOf[DenseVector[Double]]
      var i = 0
      val size = dv.size
      while (i < size) {
        dv(i) = 1.0 / (1.0 + math.exp(-dv(i)))
        i += 1
      }
      dv
    case _ => throw new RuntimeException(s"unsupported vector type ${raw.getClass}")
  }
}

case class ProbabilisticLogisticsRegressionModel(coefficientMatrix: Matrix[Double],
                                                 interceptVector: Vector[Double],
                                                 override val thresholds: Option[Array[Double]]) extends AbstractLogisticRegressionModel {
  override val numClasses: Int = interceptVector.size
  override val numFeatures: Int = coefficientMatrix.cols

  lazy val binaryModel: BinaryLogisticRegressionModel = {
    val coefficients: Vector[Double] = {
      coefficientMatrix match {
        case _dm: DenseMatrix[_] =>
          DenseVector[Double](_dm.asInstanceOf[DenseMatrix[Double]].values)
        case _sm: CSCMatrix[_] =>
          SparseVector[Double](coefficientMatrix.cols, _sm.rowIndices, _sm.asInstanceOf[CSCMatrix[Double]].values)
      }
    }
    val intercept = interceptVector(0)

    BinaryLogisticRegressionModel(coefficients = coefficients,
      intercept = intercept,
      threshold = thresholds.get(0))
  }

  private def margins(features: Vector[Double]): Vector[Double] = {
    val m = interceptVector.toDenseVector.copy
    gemv(1.0, coefficientMatrix, features.toDenseVector, 1.0, m)
    m
  }

  override def predictRaw(features: Vector[Double]): Vector[Double] = {
    margins(features)
  }

  override def rawToProbabilityInPlace(raw: Vector[Double]): Vector[Double] = {
    raw match {
      case _dv: DenseVector[_] =>
        val dv = _dv.asInstanceOf[DenseVector[Double]]
        val size = dv.size

        // get the maximum margin
        val maxMarginIndex = raw.argmax
        val maxMargin = raw(maxMarginIndex)

        if(maxMargin == Double.PositiveInfinity) {
          var k = 0
          while(k < size) {
            dv(k) = if (k == maxMarginIndex) 1.0 else 0.0
            k += 1
          }
        } else {
          val sum = {
            var temp = 0.0
            var k = 0
            while(k < numClasses) {
              dv(k) = if (maxMargin > 0) {
                math.exp(dv(k) - maxMargin)
              } else {
                math.exp(dv(k))
              }
              temp += dv(k)
              k += 1
            }
            temp
          }


          BLAS.scal(1 / sum, dv)
        }
        dv
      case _: SparseVector[_] =>
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

  override def predict(features: Vector[Double]): Double = impl.predict(features)

  override def predictRaw(features: Vector[Double]): Vector[Double] = impl.predictRaw(features)

  override def rawToProbabilityInPlace(raw: Vector[Double]): Vector[Double] = impl.rawToProbabilityInPlace(raw)
}
