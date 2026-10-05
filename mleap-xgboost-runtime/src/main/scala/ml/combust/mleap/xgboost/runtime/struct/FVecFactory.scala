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
   * Sparse inputs are read as their dense row, because that is what xgboost4j-spark scores at
   * training time (XGBoostEstimator.toXGBLabeledPoint expands the features with Vector.toArray) and
   * in XGBoostModel.transform. An index absent from a SparseVector is therefore an explicit 0.0
   * feature, not a missing one, unless the model itself treats 0.0 as missing. FVec.fromSparse
   * gives those semantics without materializing the dense row, so the cost follows the stored
   * entries rather than the vector size.
   */
  def fromSparseVector(sparseVector: SparseVector, treatsZeroAsNA: Boolean = false): FVec = {
    FVec.fromSparse(sparseVector.indices, sparseVector.values, sparseVector.size, treatsZeroAsNA)
  }

  def fromDenseVector(denseVector: DenseVector, treatsZeroAsNA: Boolean = false): FVec = {
    fromFloatArray(denseVector.values.toFloats, treatsZeroAsNA)
  }

  /** MLeap Tensor factories */
  def fromSparseTensor(sparseTensor: SparseTensor[Double], treatsZeroAsNA: Boolean = false): FVec = {
    assert(sparseTensor.dimensions.size == 1, "must provide a mono-dimensional vector")
    val indices = sparseTensor.indices.map(_.head).toArray
    FVec.fromSparse(indices, sparseTensor.values, sparseTensor.dimensions.head, treatsZeroAsNA)
  }

  def fromDenseTensor(denseTensor: Tensor[Double], treatsZeroAsNA: Boolean = false): FVec = {
    assert(denseTensor.dimensions.size == 1, "must provide a mono-dimensional vector")

    fromFloatArray(denseTensor.toArray.toFloats, treatsZeroAsNA)
  }
}
