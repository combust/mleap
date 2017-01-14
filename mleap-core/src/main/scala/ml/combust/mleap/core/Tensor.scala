package ml.combust.mleap.core

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by hollinwilkins on 1/12/17.
  */
object Tensor {
  implicit def toVector(tensor: Tensor[Double]): Vector = tensor match {
    case tensor: DenseTensor[_] if tensor.dimensions.size == 1 =>
      Vectors.dense(tensor.asInstanceOf[DenseTensor[Double]].values)
    case tensor: SparseTensor[_] if tensor.dimensions.size == 1 && tensor.dimensions.head > 0 =>
      val t = tensor.asInstanceOf[SparseTensor[Double]]
      Vectors.sparse(t.dimensions.head,
        indices = t.indices.map(_.head),
        values = t.values)
  }

  implicit def fromVector(vector: Vector): Tensor[Double] = vector match {
    case vector: DenseVector =>
      DenseTensor(vector.values, Seq(vector.values.length))
    case vector: SparseVector =>
      SparseTensor(indices = vector.indices.map(i => Array(i)),
        values = vector.values,
        dimensions = Seq(vector.size))
  }

  private val vectorDimensions: Seq[Int] = Seq(-1)
  def denseVector[T](values: Array[T]): DenseTensor[T] = DenseTensor(values, vectorDimensions)
}

sealed trait Tensor[T] {
  val dimensions: Seq[Int]

  def isDense: Boolean = false
  def isSparse: Boolean = false

  def toDense: DenseTensor[T]
  def toArray(implicit ct: ClassTag[T]): Array[T]

  def get(indices: Int *): T
}

case class DenseTensor[T](values: Array[T],
                          override val dimensions: Seq[Int]) extends Tensor[T] {
  override def isDense: Boolean = true

  override def toDense: DenseTensor[T] = this
  override def toArray(implicit ct: ClassTag[T]): Array[T] = values

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

  override def equals(obj: Any): Boolean = obj match {
    case obj: DenseTensor[_] =>
      if(values.isEmpty) {
        if(obj.values.isEmpty) { true }
        else { false }
      } else {
        values sameElements obj.asInstanceOf[DenseTensor[T]].values
      }
    case _ => false
  }
}

case class SparseTensor[T](indices: Array[Array[Int]],
                           values: Array[T],
                           override val dimensions: Seq[Int]) extends Tensor[T] {
  override def isSparse: Boolean = true

  override def toDense: DenseTensor[T] = ???
  override def toArray(implicit ct: ClassTag[T]): Array[T] = {
    val array = new Array[T](dimensions.product)
    var i = 0
    indices.foreach {
      index =>
        array(denseIndex(index)) = values(i)
        i += 1
    }
    array
  }

  override def get(indices: Int*): T = ???

  private def denseIndex(index: Array[Int]): Int = {
    var n = index.last
    var r = dimensions
    for(i <- 0 until (index.length - 1)) {
      r = r.tail
      n += index(i) * r.product
    }

    n
  }
}
