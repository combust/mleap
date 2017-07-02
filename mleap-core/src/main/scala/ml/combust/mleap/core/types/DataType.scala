package ml.combust.mleap.core.types

import scala.language.implicitConversions

object DataType {
  def apply(base: BasicType, shape: DataShape): DataType = {
    shape match {
      case ScalarShape(isNullable) => base.setNullable(isNullable)
      case ListShape(isNullable) => ListType(base).setNullable(isNullable)
      case TensorShape(dimensions, isNullable) => TensorType(base, Some(dimensions)).setNullable(isNullable)
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
sealed trait BasicType extends DataType with Serializable

case class AnyType(override val isNullable: Boolean = false) extends DataType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "any"
}
case class StringType(override val isNullable: Boolean = false) extends BasicType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "string"
}
case class BooleanType(override val isNullable: Boolean = false) extends BasicType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "boolean"
}
case class ByteType(override val isNullable: Boolean = false) extends BasicType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "byte"
}
case class ShortType(override val isNullable: Boolean = false) extends BasicType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "short"
}
case class IntegerType(override val isNullable: Boolean = false) extends BasicType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "int"
}
case class LongType(override val isNullable: Boolean = false) extends BasicType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "long"
}
case class FloatType(override val isNullable: Boolean = false) extends BasicType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "float"
}
case class DoubleType(override val isNullable: Boolean = false) extends BasicType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "double"
}
case class ByteStringType(override val isNullable: Boolean = false) extends BasicType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "byte_string"
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
  override def printString: String = s"$simpleString (base = ${base.simpleString}, nullable = $isNullable)"
}
case class ListType(base: DataType,
                    override val isNullable: Boolean = false) extends DataType {
  override def setNullable(isNullable: Boolean): DataType = copy(isNullable = isNullable)
  override def simpleString: String = "list"
  override def printString: String = s"$simpleString (base = [${base.printString}], nullable = $isNullable)"
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
