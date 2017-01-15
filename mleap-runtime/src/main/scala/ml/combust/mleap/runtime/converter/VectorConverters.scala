package ml.combust.mleap.runtime.converter

import ml.combust.mleap.tensor.{DenseTensor, SparseTensor, Tensor}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 1/15/17.
  */
trait VectorConverters {
  implicit def sparkVectorToMleapTensor(vector: Vector): Tensor[Double] = vector match {
    case vector: DenseVector => DenseTensor(vector.toArray, Seq(vector.size))
    case vector: SparseVector => SparseTensor(indices = vector.indices.map(i => Seq(i)),
      values = vector.values,
      dimensions = Seq(vector.size))
  }

  implicit def mleapTensorToSparkVector(tensor: Tensor[Double]): Vector = tensor match {
    case tensor: DenseTensor[_] =>
      Vectors.dense(tensor.rawValues.asInstanceOf[Array[Double]])
    case tensor: SparseTensor[_] =>
      Vectors.sparse(tensor.dimensions.product,
        tensor.indices.map(_.head).toArray,
        tensor.values.asInstanceOf[Array[Double]])
  }
}
object VectorConverters extends VectorConverters
