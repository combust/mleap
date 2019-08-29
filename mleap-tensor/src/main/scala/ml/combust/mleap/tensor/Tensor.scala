package ml.combust.mleap.tensor

import java.util
import java.util.Comparator

import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}

/**
  * Created by hollinwilkins on 1/12/17.
  */
object Tensor {
  val BooleanClass: Class[Boolean] = classOf[Boolean]
  val ByteClass: Class[Byte] = classOf[Byte]
  val ShortClass: Class[Short] = classOf[Short]
  val IntClass: Class[Int] = classOf[Int]
  val LongClass: Class[Long] = classOf[Long]
  val FloatClass: Class[Float] = classOf[Float]
  val DoubleClass: Class[Double] = classOf[Double]
  val StringClass: Class[String] = classOf[String]
  val ByteStringClass: Class[ByteString] = classOf[ByteString]

  def create[T: ClassTag](values: Array[T],
                          dimensions: Seq[Int],
                          indices: Option[Seq[Seq[Int]]] = None): Tensor[T] = indices match {
    case Some(is) => SparseTensor(is, values, dimensions)
    case None => DenseTensor(values, dimensions)
  }

  def denseVector[T: ClassTag](values: Array[T]): DenseTensor[T] = DenseTensor(values, Seq(values.length))
  def scalar[T: ClassTag](value: T): DenseTensor[T] = DenseTensor(Array(value), Seq())
}

sealed trait Tensor[T] {
  val dimensions: Seq[Int]
  implicit val base: ClassTag[T]

  def isDense: Boolean = false
  def isSparse: Boolean = false

  def toDense: DenseTensor[T]
  def toArray: Array[T]

  def size: Int = dimensions.product
  def rawSize: Int = rawValues.length
  def rawValues: Array[T]
  def rawValuesIterator: Iterator[T]
  def mapValues[T2: ClassTag](f: (T) => T2): Tensor[T2]

  def apply(indices: Int *): T = get(indices: _*).get
  def get(indices: Int *): Option[T]
}

case class DenseTensor[T](values: Array[T],
                          override val dimensions: Seq[Int])
                         (implicit override val base: ClassTag[T]) extends Tensor[T] {
  override def isDense: Boolean = true

  override def toDense: DenseTensor[T] = this
  override def toArray: Array[T] = values

  override def size: Int = rawValues.length

  override def rawValues: Array[T] = values
  override def rawValuesIterator: Iterator[T] = values.iterator
  override def mapValues[T2: ClassTag](f: (T) => T2): Tensor[T2] = {
    DenseTensor(values.map(f), dimensions)(classTag[T2])
  }

  override def get(indices: Int *): Option[T] = {
    var i = 0
    var dimI = 1
    var n = indices.head
    var tail = indices.tail
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

    if (values.size > n) {
      Some(values(n))
    } else { None }
  }



  override def equals(obj: Any): Boolean = obj match {
    case obj: DenseTensor[_] =>
      if(base == obj.base) {
        if (values.isEmpty) {
          if (obj.values.isEmpty) { true }
          else { false }
        } else {
          ((dimensions == obj.dimensions) ||
            ((dimensions.head == -1 || obj.dimensions.head == -1) && dimensions.tail == obj.dimensions.tail)) &&
              (values sameElements obj.asInstanceOf[DenseTensor[T]].values)
        }
      } else { false }
    case _ => false
  }
}

case class SparseTensor[T](indices: Seq[Seq[Int]],
                           values: Array[T],
                           override val dimensions: Seq[Int])
                          (implicit override val base: ClassTag[T]) extends Tensor[T] {
  override def isSparse: Boolean = true

  override def toDense: DenseTensor[T] = {
    DenseTensor(toArray, dimensions)
  }
  override def toArray: Array[T] = {
    val array = new Array[T](dimensions.product)
    var i = 0
    indices.foreach {
      index =>
        array(denseIndex(index)) = values(i)
        i += 1
    }
    array
  }

  override def rawValues: Array[T] = values
  override def rawValuesIterator: Iterator[T] = values.iterator
  override def mapValues[T2: ClassTag](f: (T) => T2): Tensor[T2] = {
    SparseTensor(indices, values.map(f), dimensions)(classTag[T2])
  }

  override def get(is: Int *): Option[T] = {

    if (is.size == dimensions.size & is.zip(dimensions).forall(x => x._1 < x._2)) {
      val index = util.Arrays.binarySearch(indices.toArray, is, new Comparator[Seq[Int]] {
        override def compare(o1: Seq[Int], o2: Seq[Int]): Int = {
          for ((v1, v2) <- o1.zip(o2)) {
            val c = v1.compareTo(v2)
            if (c != 0) {
              return c
            }
          }

          0
        }
      })

      if (index >= 0) {
        Some(values(index))
      } else {
        Some(null.asInstanceOf[T])
      }
    } else
      None
  }

  private def denseIndex(index: Seq[Int]): Int = {
    var n = index.last
    var r = dimensions
    for(i <- 0 until (index.length - 1)) {
      r = r.tail
      n += index(i) * r.product
    }

    n
  }

  override def equals(obj: Any): Boolean = obj match {
    case obj: SparseTensor[_] =>
      if(base == obj.base) {
        if (values.isEmpty) {
          if (obj.values.isEmpty) { true }
          else { false }
        } else if(indices.length == obj.indices.length && indices == obj.indices) {
          (dimensions == obj.dimensions) &&
              (values sameElements obj.asInstanceOf[SparseTensor[T]].values)
        } else { false }
      } else { false }
    case _ => false
  }
}
