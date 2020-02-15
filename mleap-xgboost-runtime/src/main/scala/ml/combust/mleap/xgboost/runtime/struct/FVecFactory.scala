package ml.combust.mleap.xgboost.runtime.struct

import biz.k11i.xgboost.util.FVec
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}

import scala.collection.JavaConversions.mapAsJavaMap


object FVecFactory {

  private def fromScalaMap(map: Map[Int, Float]): FVec = {
    val javaMap = mapAsJavaMap(map).asInstanceOf[java.util.Map[java.lang.Integer, java.lang.Float]]

    FVec.Transformer.fromMap(javaMap)
  }

  def fromSparseVector(sparseVector: SparseVector): FVec = {
    // Casting to floats, because doubles result in compounding differences from the c++ implementation
    // https://github.com/komiya-atsushi/xgboost-predictor-java/issues/21
    val scalaMap = (sparseVector.indices zip sparseVector.values.map(_.toFloat)).toMap

    FVecFactory.fromScalaMap(scalaMap)
  }

  def fromDenseVector(denseVector: DenseVector): FVec = {
    // Casting to floats, because doubles result in compounding differences from the c++ implementation
    // https://github.com/komiya-atsushi/xgboost-predictor-java/issues/21
    FVec.Transformer.fromArray(denseVector.values.map(_.toFloat), false)
  }

  def fromTensor(tensor: Tensor[Double]): FVec = FVecTensorImpl(tensor)
}
