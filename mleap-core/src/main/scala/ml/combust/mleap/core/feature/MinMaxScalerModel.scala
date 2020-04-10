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
@SparkCode(uri = "https://github.com/apache/spark/blob/v3.0.0-rc1/mllib/src/main/scala/org/apache/spark/ml/feature/MinMaxScaler.scala")
case class MinMaxScalerModel(originalMin: Vector,
                             originalMax: Vector,
                             minValue: Double = 0.0,
                             maxValue: Double = 1.0) extends Model {

  private val numFeatures = originalMax.size
  private val scale = maxValue - minValue

  // transformed value for constant cols
  private val constantOutput = (minValue + maxValue) / 2
  private val minArray = originalMin.toArray

  private val scaleArray = Array.tabulate(numFeatures) { i =>
    val range = originalMax(i) - originalMin(i)
    // scaleArray(i) == 0 iff i-th col is constant (range == 0)
    if (range != 0) scale / range else 0.0
  }

  /**Scale a feature vector using the min/max
    *
    * @param vector feature vector
    * @return scaled feature vector
    */
  def apply(vector: Vector): Vector = {
    val values = vector.toArray
    var i = 0
    while (i < numFeatures) {
      if (!values(i).isNaN) {
        if (scaleArray(i) != 0) {
          values(i) = (values(i) - minArray(i)) * scaleArray(i) + minValue
        } else {
          // scaleArray(i) == 0 means i-th col is constant
          values(i) = constantOutput
        }
      }
      i += 1
    }
    Vectors.dense(values).compressed
  }

  override def inputSchema: StructType = StructType("input" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(numFeatures)).get

}
