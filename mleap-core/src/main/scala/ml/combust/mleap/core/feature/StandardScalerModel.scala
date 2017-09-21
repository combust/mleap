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
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/StandardScaler.scala")
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
    (std, mean) match {
      case (None, None) => throw new IllegalStateException("need to scale with mean and/or with stdev")
      case (Some(stdV), None) =>
        vector match {
          case DenseVector(values) =>
            val vs = values.clone()
            val size = vs.length
            var i = 0
            while (i < size) {
              vs(i) *= (if (stdV(i) != 0.0) 1.0 / stdV(i) else 0.0)
              i += 1
            }
            Vectors.dense(vs)
          case SparseVector(size, indices, values) =>
            val vs = values.clone()
            val nnz = vs.length
            var i = 0
            while (i < nnz) {
              vs(i) *= (if (stdV(indices(i)) != 0.0) 1.0 / stdV(indices(i)) else 0.0)
              i += 1
            }
            Vectors.sparse(size, indices, vs)
        }
      case (None, Some(meanV)) =>
        vector match {
          case DenseVector(values) =>
            val vs = values.clone()
            val size = vs.length
            var i = 0
            while(i < size) {
              vs(i) -= meanV(i)
              i += 1
            }
            Vectors.dense(vs)
        }
      case (Some(stdV), Some(meanV)) =>
        vector match {
          case DenseVector(values) =>
            val vs = values.clone()
            val size = vs.length

            var i = 0
            while(i < size) {
              vs(i) = if(stdV(i) != 0.0) (vs(i) - meanV(i)) * (1.0 / stdV(i)) else 0.0
              i += 1
            }
            Vectors.dense(vs)
        }
    }
  }

  override def inputSchema: StructType = {
    StructType("input" -> TensorType.Double(size)).get
  }

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(size)).get

}