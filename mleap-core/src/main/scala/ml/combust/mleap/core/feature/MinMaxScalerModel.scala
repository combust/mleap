package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{StructType, TensorType}
import org.apache.spark.ml.linalg.mleap.VectorUtil._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import scala.math.{max, min}

/** Class for MinMax Scaler Transformer
  *
  * MinMax Scaler will use the Min/Max values to scale input features.
  *
  * @param originalMin minimum values from training features
  * @param originalMax maximum values from training features
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/MinMaxScaler.scala")
case class MinMaxScalerModel(originalMin: Vector,
                             originalMax: Vector) extends Model {
  val originalRange = (originalMax.toBreeze - originalMin.toBreeze).toArray
  val minArray = originalMin.toArray

  /**Scale a feature vector using the min/max
    *
    * @param vector feature vector
    * @return scaled feature fector
    */
  def apply(vector: Vector): Vector = {
    vector match {
      case DenseVector(values) =>
        val vs = values.clone()
        val size = vs.length
        var i = 0
        while (i < size) {
          if (!values(i).isNaN) {
            val raw = if (originalRange(i) != 0) {
              max(min(1.0, (values(i) - minArray(i)) / originalRange(i)), 0.0)
            } else {
              0.5
            }
            vs(i) = raw
          }
          i += 1
        }
        Vectors.dense(vs)
      case SparseVector(size, indices, values) =>
        val vs = values.clone()
        val nnz = vs.length
        var i = 0
        while (i < nnz) {
          val raw = if (originalRange(i) != 0) {
            max(min(1.0, (indices(i) - minArray(i)) / originalRange(i)), 0.0)
          } else {
            0.5
          }
          vs(i) *= raw
          i += 1
        }
        Vectors.sparse(size, indices, vs)
    }
  }

  override def inputSchema: StructType = StructType("input" -> TensorType.Double()).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double()).get

}