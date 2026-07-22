package ml.combust.mleap.xgboost.runtime

import com.yelp.xgboost.util.FVec
import ml.combust.mleap.tensor.{DenseTensor, SparseTensor, Tensor}
import ml.combust.mleap.xgboost.runtime.struct.FVecFactory
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.DMatrix
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}


trait XgbConverters {
  implicit class VectorOps(vector: Vector) {
    def asXGB: DMatrix = {
      vector match {
        case SparseVector(_, indices, values) =>
          new DMatrix(Iterator(new LabeledPoint(0.0f, vector.size, indices, values.map(_.toFloat))))

        case DenseVector(values) =>
          new DMatrix(Iterator(new LabeledPoint(0.0f, vector.size, null, values.map(_.toFloat))))
      }
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
    def asXGB: DMatrix = {
      tensor match {
        case SparseTensor(indices, values, _) =>
          new DMatrix(Iterator(new LabeledPoint(0.0f, tensor.size, indices.map(_.head).toArray, values.map(_.toFloat))))

        case DenseTensor(values, _) =>
          new DMatrix(Iterator(new LabeledPoint(0.0f, tensor.size, null, tensor.toDense.rawValues.map(_.toFloat))))
      }
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
