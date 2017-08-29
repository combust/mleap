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
}

case class ProbabilisticLogisticsRegressionModel(coefficientMatrix: Matrix,
                                                 interceptVector: Vector,
                                                 override val thresholds: Option[Array[Double]]) extends AbstractLogisticRegressionModel {
  override val numClasses: Int = interceptVector.size

  lazy val binaryModel: BinaryLogisticRegressionModel = {
    val coefficients = {
      require(coefficientMatrix.isTransposed,
        "LogisticRegressionModel coefficients should be row major.")
      coefficientMatrix match {
        case dm: DenseMatrix => Vectors.dense(dm.values)
        case sm: SparseMatrix => Vectors.sparse(coefficientMatrix.numCols, sm.rowIndices, sm.values)
      }
    }
    val intercept = interceptVector(0)

    BinaryLogisticRegressionModel(coefficients = coefficients,
      intercept = intercept,
      threshold = thresholds.get(0))
  }

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
  val isMultinomial: Boolean = impl.numClasses > 2

  def multinomialModel: ProbabilisticLogisticsRegressionModel = impl.asInstanceOf[ProbabilisticLogisticsRegressionModel]
  def binaryModel: BinaryLogisticRegressionModel = impl.asInstanceOf[BinaryLogisticRegressionModel]

  override def predict(features: Vector): Double = impl.predict(features)

  override def predictRaw(features: Vector): Vector = impl.predictRaw(features)

  override def rawToProbabilityInPlace(raw: Vector): Vector = impl.rawToProbabilityInPlace(raw)
}
