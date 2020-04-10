package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{StructType, TensorType}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

/** Class for standard scaler models.
  *
  * Standard scaler will use stddev, mean, or both to scale
  * a feature vector down.
  *
  * @param std optional standard deviations of features
  * @param mean optional means of features
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v3.0.0-rc1/mllib/src/main/scala/org/apache/spark/ml/feature/StandardScaler.scala")
case class StandardScalerModel(std: Option[Vector],
                               mean: Option[Vector]) extends Model {
  require(std.nonEmpty || mean.nonEmpty, "need to scale with mean and/or with stdev")

  val size = (std, mean) match {
    case (None, None) => throw new IllegalStateException("need to scale with mean and/or with stdev")
    case (Some(stdV), None) => stdV.size
    case (None, Some(meanV)) => meanV.size
    case (Some(stdV), Some(meanV)) => stdV.size
  }

  /** Scale a feature vector using stddev, mean, or both.
    *
    * @param vector feature vector
    * @return scaled feature vector
    */
  def apply(vector: Vector): Vector = {
    if (mean.nonEmpty) {
      val shift = mean.get.toArray
      val values = vector match {
        // specially handle DenseVector because its toArray does not clone already
        case d: DenseVector => d.values.clone()
        case v: SparseVector => v.toArray
      }
      val size = values.length
      if (std.nonEmpty) {
        val stdDev = std.get
        var i = 0
        while (i < size) {
          values(i) = if (stdDev(i) != 0.0) (values(i) - shift(i)) * (1.0 / stdDev(i)) else 0.0
          i += 1
        }
      } else {
        var i = 0
        while (i < size) {
          values(i) -= shift(i)
          i += 1
        }
      }
      Vectors.dense(values)
    } else if (std.nonEmpty) {
      val stdDev = std.get
      vector match {
        case DenseVector(vs) =>
          val values = vs.clone()
          val size = values.length
          var i = 0
          while(i < size) {
            values(i) *= (if (stdDev(i) != 0.0) 1.0 / stdDev(i) else 0.0)
            i += 1
          }
          Vectors.dense(values)
        case SparseVector(size, indices, vs) =>
          val values = vs.clone()
          val nnz = values.length
          var i = 0
          while (i < nnz) {
            values(i) *= (if (stdDev(indices(i)) != 0.0) 1.0 / stdDev(indices(i)) else 0.0)
            i += 1
          }
          Vectors.sparse(size, indices, values)
      }
    } else {
      throw new IllegalStateException("need to scale with mean and/or with stdev")
    }
  }

  override def inputSchema: StructType = {
    StructType("input" -> TensorType.Double(size)).get
  }

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(size)).get

}

object StandardScalerModel {

  private def transformWithBoth(
                                 shift: Array[Double],
                                 scale: Array[Double],
                                 values: Array[Double]): Array[Double] = {
    var i = 0
    while (i < values.length) {
      values(i) = (values(i) - shift(i)) * scale(i)
      i += 1
    }
    values
  }

  private def transformWithShift(
                                  shift: Array[Double],
                                  values: Array[Double]): Array[Double] = {
    var i = 0
    while (i < values.length) {
      values(i) -= shift(i)
      i += 1
    }
    values
  }

  private def transformDenseWithScale(
                                       scale: Array[Double],
                                       values: Array[Double]): Array[Double] = {
    var i = 0
    while (i < values.length) {
      values(i) *= scale(i)
      i += 1
    }
    values
  }

  private def transformSparseWithScale(
                                        scale: Array[Double],
                                        indices: Array[Int],
                                        values: Array[Double]): Array[Double] = {
    var i = 0
    while (i < values.length) {
      values(i) *= scale(indices(i))
      i += 1
    }
    values
  }

  private[feature] def getTransformFunc(
                                         shift: Array[Double],
                                         scale: Array[Double],
                                         withShift: Boolean,
                                         withScale: Boolean): Vector => Vector = {
    (withShift, withScale) match {
      case (true, true) =>
        vector: Vector =>
          val values = vector match {
            case d: DenseVector => d.values.clone()
            case v: Vector => v.toArray
          }
          val newValues = transformWithBoth(shift, scale, values)
          Vectors.dense(newValues)

      case (true, false) =>
        vector: Vector =>
          val values = vector match {
            case d: DenseVector => d.values.clone()
            case v: Vector => v.toArray
          }
          val newValues = transformWithShift(shift, values)
          Vectors.dense(newValues)

      case (false, true) =>
        vector: Vector =>
          vector match {
            case DenseVector(values) =>
              val newValues = transformDenseWithScale(scale, values.clone())
              Vectors.dense(newValues)
            case SparseVector(size, indices, values) =>
              val newValues = transformSparseWithScale(scale, indices, values.clone())
              Vectors.sparse(size, indices, newValues)
          }

      case (false, false) =>
        vector: Vector => vector
    }
  }
}
