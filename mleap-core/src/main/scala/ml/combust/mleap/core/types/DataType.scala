package ml.combust.mleap.core.types

import scala.language.implicitConversions

object DataType {
  def apply(base: BasicType, shape: DataShape): DataType = {
    shape match {
      case ScalarShape(isNullable) => ScalarType(base, isNullable = isNullable)
      case ListShape(isNullable) => ListType(base, isNullable = isNullable)
      case TensorShape(dimensions, isNullable) => TensorType(base, Some(dimensions), isNullable = isNullable)
    }
  }
}

sealed trait DataType extends Serializable {
  val isNullable: Boolean
  def asNullable: DataType = setNullable(true)
  def setNullable(isNullable: Boolean): DataType

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

case class AnyType(override val isNullable: Boolean = false) extends DataType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "any"
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

case class ScalarType(base: BasicType, override val isNullable: Boolean = false) extends DataType {
  override def setNullable(isNullable: Boolean): ScalarType = copy(isNullable = isNullable)
  override def simpleString: String = "scalar"
  override def printString: String = s"$simpleString(base=$base,nullable=$isNullable)"
}

/**
  * TensorType must have dimensions set before serializing to Bundle.ML.
  *
  * Dimensions are not necessary for a LeapFrame, but strongly encouraged
  * to ensure execution is working as expected.
 */
case class TensorType(base: BasicType,
                      dimensions: Option[Seq[Int]] = None,
                      override val isNullable: Boolean = false) extends DataType {
  def this(base: BasicType, isNullable: Boolean) = this(base, None, isNullable)

  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "tensor"
  override def printString: String = s"$simpleString(base=$base,nullable=$isNullable)"
}
case class ListType(base: BasicType,
                    override val isNullable: Boolean = false) extends DataType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "list"
  override def printString: String = s"$simpleString(base=$base,nullable=$isNullable)"
}

case class TupleType(dts: DataType *) extends DataType {
  override val isNullable: Boolean = false
  override def setNullable(isNullable: Boolean): DataType = TupleType(dts.map(_.setNullable(isNullable)): _*)

  override def simpleString: String = {
    val sb = StringBuilder.newBuilder
    sb.append("TupleDataType(")
    sb.append(dts.map(_.simpleString).mkString(","))
    sb.append(")")
    sb.toString
  }
}
