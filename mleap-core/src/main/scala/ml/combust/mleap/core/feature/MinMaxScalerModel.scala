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
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.4.0/mllib/src/main/scala/org/apache/spark/ml/feature/MinMaxScaler.scala")
case class MinMaxScalerModel(originalMin: Vector,
                             originalMax: Vector,
                             minValue: Double = 0.0,
                             maxValue: Double = 1.0) extends Model {
  val originalRange = (originalMax.toBreeze - originalMin.toBreeze).toArray
  val minArray = originalMin.toArray

  /**Scale a feature vector using the min/max
    *
    * @param vector feature vector
    * @return scaled feature vector
    */
  def apply(vector: Vector): Vector = {
    val scale = maxValue - minValue

    // 0 in sparse vector will probably be rescaled to non-zero
    val values = vector.copy.toArray
    val size = values.length
    var i = 0
    while (i < size) {
      if (!values(i).isNaN) {
        val raw = if (originalRange(i) != 0) (values(i) - minArray(i)) / originalRange(i) else 0.5
        values(i) = raw * scale + minValue
      }
      i += 1
    }
    Vectors.dense(values)
  }

  override def inputSchema: StructType = StructType("input" -> TensorType.Double(originalRange.length)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(originalRange.length)).get

}
