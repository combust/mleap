package ml.combust.mleap.core

/**
  * Created by hollinwilkins on 1/12/17.
  */
sealed trait Tensor {
  val dimensions: Seq[Int]
}

case class DenseTensor[T](values: Array[T],
                          override val dimensions: Seq[Int]) extends Tensor

case class SparseTensor[T](indices: Seq[Seq[Int]],
                           values: Array[T],
                           override val dimensions: Seq[Int]) extends Tensor
