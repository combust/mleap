package ml.combust.mleap.xgboost.runtime.struct

import com.yelp.xgboost.FVec
import ml.combust.mleap.tensor.{SparseTensor, Tensor}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}


object FVecFactory {
  /**
    *  NOTE: all these methods cast doubles to floats, because doubles result in compounding differences
    *  from the c++ implementation: https://github.com/komiya-atsushi/xgboost-predictor-java/issues/21
    */
  private implicit class ToFloatArray(doubleArray: Array[Double]) {
    def toFloats: Array[Float] = {
      doubleArray.map(_.toFloat)
    }
  }

  /**
   * treatsZeroAsNA reproduces XGBoost's missing-value handling: a model trained with missing=0.0f
   * treats a 0.0 feature as absent (routed the split's default direction), so it must be dropped
   * from the feature vector rather than compared numerically. missing=NaN (the default) needs no
   * special handling because the FVec impl already treats NaN as absent.
   */
  private def fromFloatArray(values: Array[Float], treatsZeroAsNA: Boolean): FVec =
    if (treatsZeroAsNA) FVec.fromArrayWithZeroAsMissing(values) else FVec.fromArray(values)

  /**
   * An index absent from a sparse input is missing, and a stored entry is a value. That is how
   * XGBoost 1.7.6 trained and predicted, and how an xgboost4j-spark that hands the stored indices and
   * values to the DMatrix trains and predicts. A model learns a
   * missing feature as the default direction of each split, so reading a gap as 0.0 sends rows that
   * are mostly gaps down the same trees. When the model treats 0.0 as missing (missing=0.0f) a stored
   * 0.0 is missing as well, so it is left out.
   */
  private def fromSparseEntries(indices: Array[Int], values: Array[Double], treatsZeroAsNA: Boolean): FVec = {
    val entries = new java.util.HashMap[Integer, java.lang.Double](indices.length * 2)
    var i = 0
    while (i < indices.length) {
      if (!(treatsZeroAsNA && values(i) == 0.0)) entries.put(indices(i), values(i))
      i += 1
    }
    FVec.fromMap(entries)
  }

  def fromSparseVector(sparseVector: SparseVector, treatsZeroAsNA: Boolean = false): FVec =
    fromSparseEntries(sparseVector.indices, sparseVector.values, treatsZeroAsNA)

  def fromDenseVector(denseVector: DenseVector, treatsZeroAsNA: Boolean = false): FVec = {
    fromFloatArray(denseVector.values.toFloats, treatsZeroAsNA)
  }

  /** MLeap Tensor factories */
  def fromSparseTensor(sparseTensor: SparseTensor[Double], treatsZeroAsNA: Boolean = false): FVec = {
    assert(sparseTensor.dimensions.size == 1, "must provide a mono-dimensional vector")
    fromSparseEntries(sparseTensor.indices.map(_.head).toArray, sparseTensor.values, treatsZeroAsNA)
  }

  def fromDenseTensor(denseTensor: Tensor[Double], treatsZeroAsNA: Boolean = false): FVec = {
    assert(denseTensor.dimensions.size == 1, "must provide a mono-dimensional vector")

    fromFloatArray(denseTensor.toArray.toFloats, treatsZeroAsNA)
  }
}
