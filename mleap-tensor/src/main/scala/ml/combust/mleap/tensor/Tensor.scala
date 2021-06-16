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
                          indices: Option[Seq[Seq[Int]]] = None): Tensor[T] = {
    indices match {
      case Some(is) => {
        if (!isKnownDimensions(dimensions))
          throw new IllegalArgumentException("dimensions must be known for SparseTensor")
        SparseTensor(is, values, dimensions)
      }
      case None => {
        DenseTensor(values, normalizeDimensions(values.length, dimensions))
      }
    }
  }

  def denseVector[T: ClassTag](values: Array[T]): DenseTensor[T] = DenseTensor(values, Seq(values.length))
  def scalar[T: ClassTag](value: T): DenseTensor[T] = DenseTensor(Array(value), Seq())
  def denseIndex(indices: Seq[Int], dimensions: Seq[Int]): Int = {
    var n = indices.last
    var r = dimensions
    for(i <- 0 until (indices.length - 1)) {
      r = r.tail
      n += indices(i) * r.product
    }
    n
  }
  def isKnownDimensions(dimensions: Seq[Int]): Boolean =  dimensions.count(dim => dim == -1) == 0

  def normalizeDimensions(size: Int, dimensions: Seq[Int]) : Seq[Int] = {
    val numOfUnknownDimensions = dimensions.count(dim => dim == -1)
    if (numOfUnknownDimensions > 1)
      throw new  IllegalArgumentException("dimensions contains more then one `-1`")

    val normalizedDimensions = if (numOfUnknownDimensions == 1) {
      val concreteDimension = size / dimensions.filter(dim => dim != -1 ).product
      dimensions.map(dim =>  if(dim == -1) concreteDimension else dim)
    } else {
      dimensions
    }
    if(normalizedDimensions.product != size)
      throw  new IllegalArgumentException("size of dimensions must equals size of values")

    normalizedDimensions
  }
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

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: Tensor[T] =>
        base == obj.base &&
          dimensions == obj.dimensions &&
          rawValues.sameElements[T](obj.rawValues)
      case _ => false
    }
  }
}

case class DenseTensor[T](values: Array[T],
                          override val dimensions: Seq[Int])
                         (implicit override val base: ClassTag[T]) extends Tensor[T] {
  override def isDense: Boolean = true
  override def toDense: DenseTensor[T] = this
  override def toArray: Array[T] = values

  override def rawValues: Array[T] = values
  override def rawValuesIterator: Iterator[T] = values.iterator
  override def mapValues[T2: ClassTag](f: (T) => T2): Tensor[T2] = {
    DenseTensor(values.map(f), dimensions)(classTag[T2])
  }

  override def get(indices: Int *): Option[T] = {
    val n = Tensor.denseIndex(indices, dimensions)
    if (values.length > n) {
      Some(values(n))
    } else { None }
  }

  override def equals(obj: Any): Boolean = obj match {
    case obj: DenseTensor[T] => super.equals(obj)
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
        array(Tensor.denseIndex(index, dimensions)) = values(i)
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
    val index = util.Arrays.binarySearch(indices.toArray, is, new Comparator[Seq[Int]] {
      override def compare(o1: Seq[Int], o2: Seq[Int]): Int = {
        for((v1, v2) <- o1.zip(o2)) {
          val c = v1.compareTo(v2)
          if(c != 0) { return c }
        }

        0
      }
    })

    if(index >= 0) {
      Some(values(index))
    } else { None }
  }

  override def equals(obj: Any): Boolean = obj match {
    case obj: SparseTensor[T] =>
        indices == obj.indices && super.equals(obj)
    case _ => false
  }
}
