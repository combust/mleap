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

case class DenseTensor[T](values: Array[T],
                          override val dimensions: Seq[Int]) extends Tensor[T] {
  override def toDense: DenseTensor[T] = this

  override def get(indices: Int *): T = {
    var i = 0
    var dimI = 1
    var head :: tail = indices
    var n = head
    while(i < tail.size) {
      var ti = dimI
      var tn = tail.head
      tail = tail.tail
      while(ti < dimensions.size) {
        tn *= dimensions(ti)
        ti += 1
      }
      dimI += 1
      i += 1
      n += tn
    }

    values(n)
  }
}

case class SparseTensor[T](indices: Array[Array[Int]],
                           values: AnyRef,
                           override val dimensions: Seq[Int]) extends Tensor[T] {
  override def toDense: DenseTensor[T] = ???

  override def get(indices: Int*): T = ???
}
