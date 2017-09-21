package ml.combust.mleap.core.types

import scala.collection.JavaConverters._
import scala.language.implicitConversions

object DataType {
  def apply(base: BasicType, shape: DataShape): DataType = {
    shape match {
      case ScalarShape(isNullable) => ScalarType(base, isNullable = isNullable)
      case ListShape(isNullable) => ListType(base, isNullable = isNullable)
      case TensorShape(dimensions, isNullable) => TensorType(base, dimensions, isNullable = isNullable)
    }
  }
}

sealed trait DataType extends Serializable {
  val isNullable: Boolean

  def asNullable: DataType = setNullable(true)
  def nonNullable: DataType = setNullable(false)
  def setNullable(isNullable: Boolean): DataType

  def base: BasicType
  def shape: DataShape

  def simpleString: String
  def printString: String = s"$simpleString (nullable = $isNullable)"
}
sealed trait BasicType extends Serializable

object BasicType {
  case object Boolean extends BasicType {
    override def toString: String = "boolean"
  }
  case object Byte extends BasicType {
    override def toString: String = "byte"
  }
  case object Short extends BasicType {
    override def toString: String = "short"
  }
  case object Int extends BasicType {
    override def toString: String = "int"
  }
  case object Long extends BasicType {
    override def toString: String = "long"
  }
  case object Float extends BasicType {
    override def toString: String = "float"
  }
  case object Double extends BasicType {
    override def toString: String = "double"
  }
  case object String extends BasicType {
    override def toString: String = "string"
  }
  case object ByteString extends BasicType {
    override def toString: String = "byte_string"
  }
}

object ScalarType {
  val Boolean = ScalarType(BasicType.Boolean)
  val Byte = ScalarType(BasicType.Byte)
  val Short = ScalarType(BasicType.Short)
  val Int = ScalarType(BasicType.Int)
  val Long = ScalarType(BasicType.Long)
  val Float = ScalarType(BasicType.Float)
  val Double = ScalarType(BasicType.Double)
  val String = ScalarType(BasicType.String)
  val ByteString = ScalarType(BasicType.ByteString)
}

case class ScalarType(override val base: BasicType, override val isNullable: Boolean = true) extends DataType {
  override def setNullable(isNullable: Boolean): ScalarType = copy(isNullable = isNullable)
  override def simpleString: String = "scalar"
  override def printString: String = s"$simpleString(base=$base,nullable=$isNullable)"

  override def shape: ScalarShape = ScalarShape(isNullable)
}

object TensorType {
  def apply(base: BasicType,
            dimensions: Seq[Int]): TensorType = TensorType(base, Some(dimensions))

  def Double(dims: Int *): TensorType = TensorType(BasicType.Double, Some(dims))
}

/**
  * TensorType must have dimensions set before serializing to Bundle.ML.
  *
  * Dimensions are not necessary for a LeapFrame, but strongly encouraged
  * to ensure execution is working as expected.
 */
case class TensorType(override val base: BasicType,
                      dimensions: Option[Seq[Int]] = None,
                      override val isNullable: Boolean = true) extends DataType {
  def this(base: BasicType, isNullable: Boolean) = this(base, None, isNullable)

  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "tensor"
  override def printString: String = s"$simpleString(base=$base,nullable=$isNullable)"

  override def shape: TensorShape = TensorShape(dimensions, isNullable)
}

object ListType {
  val Boolean = ListType(BasicType.Boolean)
  val Byte = ListType(BasicType.Byte)
  val Int = ListType(BasicType.Int)
  val Short = ListType(BasicType.Short)
  val Long = ListType(BasicType.Long)
  val Float = ListType(BasicType.Float)
  val Double = ListType(BasicType.Double)
  val String = ListType(BasicType.String)
  val ByteString = ListType(BasicType.ByteString)
}

case class ListType(override val base: BasicType,
                    override val isNullable: Boolean = true) extends DataType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "list"
  override def printString: String = s"$simpleString(base=$base,nullable=$isNullable)"

  override def shape: ListShape = ListShape(isNullable)
}
