package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{StructType, TensorType}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import scala.collection.mutable

/**
  * Created by hollinwilkins on 12/27/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v3.2.0/mllib/src/main/scala/org/apache/spark/mllib/feature/ChiSqSelector.scala")
case class ChiSqSelectorModel(filterIndices: Seq[Int],
                              inputSize: Int) extends Model {
  private val sortedFilterIndices = filterIndices.sorted
  def apply(features: Vector): Vector = {
    features match {
      case SparseVector(size, indices, values) =>
        val newSize = sortedFilterIndices.length
        val newValues = mutable.ArrayBuilder.make[Double]
        val newIndices = mutable.ArrayBuilder.make[Int]
        var i = 0
        var j = 0
        var indicesIdx = 0
        var filterIndicesIdx = 0
        while (i < indices.length && j < sortedFilterIndices.length) {
          indicesIdx = indices(i)
          filterIndicesIdx = sortedFilterIndices(j)
          if (indicesIdx == filterIndicesIdx) {
            newIndices += j
            newValues += values(i)
            j += 1
            i += 1
          } else {
            if (indicesIdx > filterIndicesIdx) {
              j += 1
            } else {
              i += 1
            }
          }
        }
        // TODO: Sparse representation might be ineffective if (newSize ~= newValues.size)
        Vectors.sparse(newSize, newIndices.result(), newValues.result())
      case DenseVector(values) =>
        val values = features.toArray
        Vectors.dense(sortedFilterIndices.map(i => values(i)).toArray)
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }

  override def inputSchema: StructType = StructType("input" -> TensorType.Double(inputSize)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(sortedFilterIndices.length)).get
}
