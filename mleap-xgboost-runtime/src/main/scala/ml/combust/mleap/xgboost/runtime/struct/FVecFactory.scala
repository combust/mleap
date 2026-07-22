package ml.combust.mleap.xgboost.runtime.struct

import java.{lang, util}

import com.yelp.xgboost.util.FVec
import ml.combust.mleap.tensor.{SparseTensor, Tensor}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}

import scala.collection.JavaConverters.mapAsJavaMapConverter


object FVecFactory {
  private def toJavaMap(map: Map[Int, Float]): util.Map[lang.Integer, lang.Float] = {
    map.asJava.asInstanceOf[util.Map[lang.Integer, lang.Float]]
  }

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
  private def dropZeros(map: Map[Int, Float], treatsZeroAsNA: Boolean): Map[Int, Float] =
    if (treatsZeroAsNA) map.filter(_._2 != 0.0f) else map

  /** Vector factories */
  def fromSparseVector(sparseVector: SparseVector, treatsZeroAsNA: Boolean = false): FVec = {
    val scalaMap = dropZeros((sparseVector.indices zip sparseVector.values.toFloats).toMap, treatsZeroAsNA)
    FVec.Transformer.fromMap(toJavaMap(scalaMap))
  }

  def fromDenseVector(denseVector: DenseVector, treatsZeroAsNA: Boolean = false): FVec = {
    FVec.Transformer.fromArray(denseVector.values.toFloats, treatsZeroAsNA)
  }

  /** MLeap Tensor factories */
  def fromSparseTensor(sparseTensor: SparseTensor[Double], treatsZeroAsNA: Boolean = false): FVec = {
    assert(sparseTensor.dimensions.size == 1, "must provide a mono-dimensional vector")
    val indices = sparseTensor.indices.map(_.head).toArray[Int]

    val scalaMap = dropZeros((indices zip sparseTensor.values.toFloats).toMap, treatsZeroAsNA)
    FVec.Transformer.fromMap(toJavaMap(scalaMap))
  }

  def fromDenseTensor(denseTensor: Tensor[Double], treatsZeroAsNA: Boolean = false): FVec = {
    assert(denseTensor.dimensions.size == 1, "must provide a mono-dimensional vector")

    FVec.Transformer.fromArray(denseTensor.toArray.toFloats, treatsZeroAsNA)
  }
}
