package ml.combust.mleap.xgboost.runtime

import com.yelp.xgboost.FVec
import ml.combust.mleap.tensor.{DenseTensor, SparseTensor, Tensor}
import ml.combust.mleap.xgboost.runtime.struct.FVecFactory
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.DMatrix
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}


/**
  * Sparse inputs keep their gaps. An index absent from a SparseVector or sparse Tensor is missing and a
  * stored entry is a value, as in XGBoost 1.7.6 and in an xgboost4j-spark whose LabeledPoint carries
  * the stored indices and values. The model's missing
  * value is handed to the DMatrix, so with missing=0.0f the stored zeros are missing too.
  */
trait XgbConverters {
  implicit class VectorOps(vector: Vector) {
    def asXGB(missing: Float = Float.NaN): DMatrix = {
      val point = vector match {
        case sparse: SparseVector =>
          new LabeledPoint(0.0f, sparse.size, sparse.indices, sparse.values.map(_.toFloat))
        case dense => new LabeledPoint(0.0f, dense.size, null, dense.toArray.map(_.toFloat))
      }
      new DMatrix(Iterator(point), null, missing)
    }

    def asXGBPredictor(treatsZeroAsNA: Boolean = false): FVec = {
      vector match {
        case sparseVector: SparseVector =>
          FVecFactory.fromSparseVector(sparseVector, treatsZeroAsNA)
        case denseVector: DenseVector =>
          FVecFactory.fromDenseVector(denseVector, treatsZeroAsNA)
      }
    }
  }

  implicit class DoubleTensorOps(tensor: Tensor[Double]) {
    def asXGB(missing: Float = Float.NaN): DMatrix = {
      val point = tensor match {
        case sparse: SparseTensor[Double] =>
          new LabeledPoint(
            0.0f, sparse.dimensions.head, sparse.indices.map(_.head).toArray, sparse.values.map(_.toFloat))
        case dense => new LabeledPoint(0.0f, dense.size, null, dense.toArray.map(_.toFloat))
      }
      new DMatrix(Iterator(point), null, missing)
    }

    def asXGBPredictor(treatsZeroAsNA: Boolean = false): FVec = {
      tensor match {
        case sparseTensor: SparseTensor[Double] =>
          FVecFactory.fromSparseTensor(sparseTensor, treatsZeroAsNA)

        case denseTensor: DenseTensor[Double] =>
          FVecFactory.fromDenseTensor(denseTensor, treatsZeroAsNA)
      }
    }
  }
}

object XgbConverters extends XgbConverters
