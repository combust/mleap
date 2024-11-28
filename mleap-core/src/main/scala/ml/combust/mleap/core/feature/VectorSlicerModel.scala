package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{StructType, TensorType}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.VectorUtil._

/**
  * Created by hollinwilkins on 12/28/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/VectorSlicer.scala")
case class VectorSlicerModel(indices: Array[Int],
                             namedIndices: Array[(String, Int)] = Array(),
                            inputSize: Int) extends Model {
  val allIndices: Array[Int] = indices.union(namedIndices.map(_._2)).toArray

  def apply(features: Vector): Vector = features match {
    case features: DenseVector => Vectors.dense(allIndices.map(features.apply))
    case features: SparseVector => features.slice(allIndices)
  }

  override def inputSchema: StructType = StructType("input" -> TensorType.Double(inputSize)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(allIndices.length)).get

}
