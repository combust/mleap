package ml.combust.mleap.xgboost.runtime

import com.yelp.xgboost.FVec
import ml.combust.mleap.tensor.{DenseTensor, SparseTensor, Tensor}
import ml.combust.mleap.xgboost.runtime.struct.FVecFactory
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.DMatrix
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}


/**
  * Sparse inputs are densified and the model's missing value is handed to the DMatrix, which is what
  * xgboost4j-spark does at training time (XGBoostEstimator.toXGBLabeledPoint expands the features
  * with Vector.toArray) and in XGBoostModel.transform. An index absent from a SparseVector is
  * therefore an explicit 0.0 feature, and only the model's missing value decides whether XGBoost
  * reads it as absent.
  */
trait XgbConverters {
  implicit class VectorOps(vector: Vector) {
    def asXGB(missing: Float = Float.NaN): DMatrix = {
      new DMatrix(
        Iterator(new LabeledPoint(0.0f, vector.size, null, vector.toArray.map(_.toFloat))), null, missing)
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
      new DMatrix(
        Iterator(new LabeledPoint(0.0f, tensor.size, null, tensor.toArray.map(_.toFloat))), null, missing)
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
