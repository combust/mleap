package ml.combust.mleap.core

import java.lang.reflect

/**
  * Created by hollinwilkins on 1/12/17.
  */
sealed trait Tensor[T] {
  val dimensions: Seq[Int]

  def toDense: DenseTensor[T]

  def get(indices: Int *): T
}

case class DenseTensor[T](values: AnyRef,
                          override val dimensions: Seq[Int]) extends Tensor[T] {
  override def toDense: DenseTensor[T] = this

  override def get(indices: Int *): T = {
    var arr = values
    var i = 0
    val end = indices.size - 1
    while(i < end) {
      arr = reflect.Array.get(arr, indices(i))
      i += 1
    }

    reflect.Array.get(arr, indices.last).asInstanceOf[T]
  }
}

case class SparseTensor[T](indices: Array[Array[Int]],
                           values: AnyRef,
                           override val dimensions: Seq[Int]) extends Tensor[T] {
  override def toDense: DenseTensor[T] = ???

  override def get(indices: Int*): T = ???
}
