package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{StructField, StructType, TensorType}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

/** Class for an element wise product model.
  *
  * @param scalingVec vector for scaling feature vectors
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/ElementwiseProduct.scala")
case class ElementwiseProductModel(scalingVec: Vector) extends Model {
  def apply(vector: Vector): Vector = {
    vector match {
      case DenseVector(values) =>
        val vs = values.clone()
        val size = vs.length
        var i = 0

        while (i < size) {
          vs(i) *= scalingVec(i)
          i += 1
        }
        Vectors.dense(vs)
      case SparseVector(size, indices, values) =>
        val vs = values.clone()
        val nnz = vs.length
        var i = 0
        while (i < nnz) {
          vs(i) *= scalingVec(indices(i))
          i += 1
        }
        Vectors.sparse(size, indices, vs)
    }
  }

  override def inputSchema: StructType = StructType(StructField("input" -> TensorType.Double(scalingVec.size))).get

  override def outputSchema: StructType = StructType(StructField("output" -> TensorType.Double(scalingVec.size))).get
}
