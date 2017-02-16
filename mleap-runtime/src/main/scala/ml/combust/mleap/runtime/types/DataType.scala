package ml.combust.mleap.runtime.types

import ml.combust.bundle
import spray.json.RootJsonFormat
import scala.language.implicitConversions

object DataType {
  implicit def apply[T](ct: bundle.custom.CustomType[T]): CustomType = {
    CustomType(ct.asInstanceOf[bundle.custom.CustomType[Any]])
  }
}

sealed trait DataType extends Serializable {
  val isNullable: Boolean
  def asNullable: DataType

  def simpleString: String
  def printString: String = s"$simpleString (nullable = $isNullable)"
}
sealed trait BasicType extends DataType with Serializable

case class AnyType(override val isNullable: Boolean = false) extends DataType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "any"
}
case class StringType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "string"
}
case class BooleanType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "boolean"
}
case class ByteType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "byte"
}
case class ShortType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "short"
}
case class IntegerType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "int"
}
case class LongType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "long"
}
case class FloatType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "float"
}
case class DoubleType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "double"
}
case class ByteStringType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "byte_string"
}

case class TensorType(base: BasicType,
                      override val isNullable: Boolean = false) extends DataType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "tensor"
  override def printString: String = s"$simpleString (base = ${base.simpleString}, nullable = $isNullable)"
}
case class ListType(base: DataType,
                    override val isNullable: Boolean = false) extends DataType {
  override def asNullable: DataType = copy(isNullable = true)
  override def simpleString: String = "list"
  override def printString: String = s"$simpleString (base = [${base.printString}], nullable = $isNullable)"
}

object CustomType {
  implicit def apply[T](ct: bundle.custom.CustomType[T]): CustomType = {
    new CustomType(ct.asInstanceOf[bundle.custom.CustomType[Any]])
  }
}

case class CustomType private (ct: bundle.custom.CustomType[Any],
                          override val isNullable: Boolean = false) extends DataType with bundle.custom.CustomType[Any] {
  override def asNullable: DataType = copy(isNullable = true)

  override val klazz: Class[Any] = ct.klazz
  override def name: String = ct.name
  override def format: RootJsonFormat[Any] = ct.format
  override def simpleString: String = "custom"
  override def printString: String = s"$simpleString (name = $name, nullable = $isNullable)"

  override def isSmall(obj: Any): Boolean = ct.isSmall(obj)
  override def isLarge(obj: Any): Boolean = ct.isLarge(obj)
  override def toJsonBytes(obj: Any): Array[Byte] = ct.toJsonBytes(obj)
  override def fromJsonBytes(bytes: Array[Byte]): Any = ct.fromJsonBytes(bytes)
  override def toBytes(obj: Any): Array[Byte] = ct.toBytes(obj)
  override def fromBytes(bytes: Array[Byte]): Any = ct.fromBytes(bytes)
}
