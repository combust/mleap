package ml.combust.mleap.core.tensor

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by hollinwilkins on 1/12/17.
  */
object Tensor {
  val BOOLEAN: Byte = 0
  val STRING: Byte = 1
  val INT: Byte = 2
  val LONG: Byte = 3
  val FLOAT: Byte = 4
  val DOUBLE: Byte = 5

  implicit def toVector(tensor: Tensor[Double]): Vector = tensor match {
    case tensor: DenseTensor[_] if tensor.dimensions.size == 1 =>
      Vectors.dense(tensor.asInstanceOf[DenseTensor[Double]].values)
    case tensor: SparseTensor[_] if tensor.dimensions.size == 1 && tensor.dimensions.head > 0 =>
      val t = tensor.asInstanceOf[SparseTensor[Double]]
      Vectors.sparse(t.dimensions.head,
        indices = t.indices.map(_.head).toArray,
        values = t.values)
  }

  implicit def fromVector(vector: Vector): Tensor[Double] = vector match {
    case vector: DenseVector =>
      DenseTensor(DOUBLE, vector.values, Seq(vector.values.length))
    case vector: SparseVector =>
      SparseTensor(DOUBLE, indices = vector.indices.map(i => Seq(i)),
        values = vector.values,
        dimensions = Seq(vector.size))
  }

  private val BooleanClass = classOf[Boolean]
  private val StringClass = classOf[String]
  private val IntClass = classOf[Int]
  private val LongClass = classOf[Long]
  private val FloatClass = classOf[Float]
  private val DoubleClass = classOf[Double]

  def tensorType[T: ClassTag]: Byte = implicitly[ClassTag[T]].runtimeClass match {
    case BooleanClass => BOOLEAN
    case StringClass => STRING
    case IntClass => INT
    case LongClass => LONG
    case FloatClass => FLOAT
    case DoubleClass => DOUBLE
    case _ => throw new RuntimeException(s"unsupported class ${implicitly[ClassTag[T]].runtimeClass}")
  }

  def denseVector[T: ClassTag](values: Array[T]): DenseTensor[T] = DenseTensor(tensorType[T], values, Seq(values.length))
}

sealed trait Tensor[T] {
  val dimensions: Seq[Int]
  val base: Byte

  def isDense: Boolean = false
  def isSparse: Boolean = false

  def toDense: DenseTensor[T]
  def toArray(implicit ct: ClassTag[T]): Array[T]

  def get(indices: Int *): T
}

object DenseTensor {
  def apply[T: ClassTag](values: Array[T],
                         dimensions: Seq[Int]): DenseTensor[T] = {
    DenseTensor(Tensor.tensorType[T], values, dimensions)
  }
}

case class DenseTensor[T](override val base: Byte,
                          values: Array[T],
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
      if(base == obj.base) {
        if (values.isEmpty) {
          if (obj.values.isEmpty) { true }
          else { false }
        } else {
          (dimensions == obj.dimensions) ||
            (dimensions.head == -1 || obj.dimensions.head == -1 && dimensions.tail == obj.dimensions.tail) &&
              (values sameElements obj.asInstanceOf[DenseTensor[T]].values)
        }
      } else { false }
    case _ => false
  }
}

object SparseTensor {
  def apply[T: ClassTag](indices: Seq[Seq[Int]],
                         values: Array[T],
                         dimensions: Seq[Int]): SparseTensor[T] = {
    SparseTensor(Tensor.tensorType[T], indices, values, dimensions)
  }
}

case class SparseTensor[T](override val base: Byte,
                           indices: Seq[Seq[Int]],
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
          (dimensions == obj.dimensions) ||
            (dimensions.head == -1 || obj.dimensions.head == -1 && dimensions.tail == obj.dimensions.tail) &&
              (values sameElements obj.asInstanceOf[SparseTensor[T]].values)
        } else { false }
      } else { false }
    case _ => false
  }
}
