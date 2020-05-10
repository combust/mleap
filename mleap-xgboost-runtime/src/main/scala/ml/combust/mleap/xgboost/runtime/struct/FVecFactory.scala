package ml.combust.mleap.xgboost.runtime.struct

import java.{lang, util}

import biz.k11i.xgboost.util.FVec
import ml.combust.mleap.tensor.{SparseTensor, Tensor}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}

import scala.collection.JavaConversions.mapAsJavaMap


object FVecFactory {
  private def toJavaMap(map: Map[Int, Float]): util.Map[lang.Integer, lang.Float] = {
    mapAsJavaMap(map).asInstanceOf[util.Map[lang.Integer, lang.Float]]
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

  /** Vector factories */
  def fromSparseVector(sparseVector: SparseVector): FVec = {
    val scalaMap = (sparseVector.indices zip sparseVector.values.toFloats).toMap
    FVec.Transformer.fromMap(toJavaMap(scalaMap))
  }

  def fromDenseVector(denseVector: DenseVector): FVec = {
    FVec.Transformer.fromArray(denseVector.values.toFloats, false)
  }

  /** MLeap Tensor factories */
  def fromSparseTensor(sparseTensor: SparseTensor[Double]): FVec = {
    assert(sparseTensor.dimensions.size == 1, "must provide a mono-dimensional vector")
    val indices = sparseTensor.indices.map(_.head).toArray[Int]

    val scalaMap = (indices zip sparseTensor.values.toFloats).toMap
    FVec.Transformer.fromMap(toJavaMap(scalaMap))
  }

  def fromDenseTensor(denseTensor: Tensor[Double]): FVec = {
    assert(denseTensor.dimensions.size == 1, "must provide a mono-dimensional vector")

    FVec.Transformer.fromArray(denseTensor.toArray.toFloats, false)
  }
}
