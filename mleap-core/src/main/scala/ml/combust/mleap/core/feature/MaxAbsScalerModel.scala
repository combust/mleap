package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{StructType, TensorType}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import scala.math.{max, min}

/** Class for MaxAbs Scaler model.
  *
  * @param maxAbs max absolute value
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/MaxAbsScaler.scala")
case class MaxAbsScalerModel(maxAbs: Vector) extends Model {
  private val scale = maxAbs.toArray.map { v => if (v == 0) 1.0 else 1 / v }
  private val func = StandardScalerModel.getTransformFunc(
    Array.empty, scale, withShift = false, withScale = true
  )

  def apply(vector: Vector): Vector = func(vector)

  override def inputSchema: StructType = StructType("input" -> TensorType.Double(maxAbs.size)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(maxAbs.size)).get

}
