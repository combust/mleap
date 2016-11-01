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
  def fits(other: DataType): Boolean = this == other
}
sealed trait BasicType extends DataType with Serializable

object AnyType extends DataType {
  override def fits(other: DataType): Boolean = true
}

object IntegerType extends BasicType
object LongType extends BasicType
object BooleanType extends BasicType
object DoubleType extends BasicType
object StringType extends BasicType

case class TensorType(base: BasicType, dimensions: Seq[Int]) extends DataType {
  override def fits(other: DataType): Boolean = {
    if(super.fits(other)) { return true }

    other match {
      case TensorType(ob, od) => base == ob && dimFit(od)
      case _ => false
    }
  }

  private def dimFit(d2: Seq[Int]): Boolean = {
    if(dimensions.length == d2.length) {
      for((dd1, dd2) <- dimensions.zip(d2)) {
        if(dd1 != -1 && dd1 != dd2) { return false }
      }
      true
    } else { false }
  }
}
case class ArrayType(base: DataType) extends DataType

object TensorType {
  def doubleVector(dim: Int = -1): TensorType = TensorType(DoubleType, Seq(dim))
}

object CustomType {
  implicit def apply[T](ct: bundle.custom.CustomType[T]): CustomType = {
    new CustomType(ct.asInstanceOf[bundle.custom.CustomType[Any]])
  }
}

class CustomType private (ct: bundle.custom.CustomType[Any]) extends DataType with bundle.custom.CustomType[Any] {
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
