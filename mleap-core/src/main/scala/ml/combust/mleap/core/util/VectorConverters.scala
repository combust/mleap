package ml.combust.mleap.core.util

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import ml.combust.mleap.tensor.{DenseTensor, SparseTensor, Tensor}
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrices, Matrix, SparseMatrix, SparseVector, Vector, Vectors}

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

  implicit def sparkMatrixToMleapTensor(matrix: Matrix): Tensor[Double] = matrix match {
    case matrix: DenseMatrix =>
      DenseTensor(matrix.toArray, Seq(matrix.numRows, matrix.numCols))
    case matrix: SparseMatrix =>
      val indices = matrix.rowIndices.zip(matrix.colPtrs).map {
        case (r, c) => Seq(r, c)
      }.toSeq
      SparseTensor(indices = indices,
      values = matrix.values,
      dimensions = Seq(matrix.numRows, matrix.numCols))
  }

  implicit def mleapTensorToSparkMatrix(tensor: Tensor[Double]): Matrix = tensor match {
    case tensor: DenseTensor[_] =>
      Matrices.dense(tensor.dimensions.head,
        tensor.dimensions(1),
        tensor.rawValues.asInstanceOf[Array[Double]])
    case tensor: SparseTensor[_] =>
      val (rows, cols) = tensor.indices.map(v => (v.head, v(1))).unzip
      Matrices.sparse(tensor.dimensions.head,
        tensor.dimensions(1),
        cols.toArray,
        rows.toArray,
        tensor.values.asInstanceOf[Array[Double]])
  }

  implicit def breezeVectorToMLeapTensor(vector: BV[Double]): Tensor[Double] = vector match {
    case vector : BDV[Double] => DenseTensor(vector.toArray, Seq(vector.size))
    case vector : BSV[Double] => SparseTensor(vector.index.map(i => Seq(i)), vector.data, Seq(vector.values.size))
  }


  implicit def mleapTensorToBreezeVector(tensor: Tensor[Double]): BV[Double] = tensor match {
    case tensor: DenseTensor[_] =>
      new BDV(tensor.rawValues.asInstanceOf[Array[Double]])
    case tensor: SparseTensor[_] =>
      new BSV(tensor.indices.map(_.head).toArray,
        tensor.values.asInstanceOf[Array[Double]],
        tensor.dimensions.product)
  }
}
object VectorConverters extends VectorConverters
