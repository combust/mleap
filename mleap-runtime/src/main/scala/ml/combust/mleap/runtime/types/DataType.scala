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
}
sealed trait BasicType extends DataType with Serializable

case class AnyType(override val isNullable: Boolean = false) extends DataType {
  override def asNullable: DataType = copy(isNullable = true)
}

case class IntegerType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
}
case class LongType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
}
case class BooleanType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
}
case class FloatType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
}
case class DoubleType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
}
case class StringType(override val isNullable: Boolean = false) extends BasicType {
  override def asNullable: DataType = copy(isNullable = true)
}

case class TensorType(base: BasicType,
                      override val isNullable: Boolean = false) extends DataType {
  override def asNullable: DataType = copy(isNullable = true)
}
case class ListType(base: DataType,
                    override val isNullable: Boolean = false) extends DataType {
  override def asNullable: DataType = copy(isNullable = true)
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

  override def isSmall(obj: Any): Boolean = ct.isSmall(obj)
  override def isLarge(obj: Any): Boolean = ct.isLarge(obj)
  override def toJsonBytes(obj: Any): Array[Byte] = ct.toJsonBytes(obj)
  override def fromJsonBytes(bytes: Array[Byte]): Any = ct.fromJsonBytes(bytes)
  override def toBytes(obj: Any): Array[Byte] = ct.toBytes(obj)
  override def fromBytes(bytes: Array[Byte]): Any = ct.fromBytes(bytes)
}
