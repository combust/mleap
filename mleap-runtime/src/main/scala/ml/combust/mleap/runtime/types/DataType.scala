package ml.combust.mleap.runtime.types

import spray.json.{JsValue, JsonFormat}

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
case class ListType(base: DataType) extends DataType

object TensorType {
  def doubleVector(dim: Int = -1): TensorType = TensorType(DoubleType, Seq(dim))
}

trait CustomType[T] extends DataType {
  import spray.json._

  val klazz: Class[T]
  val name: String
  val format: JsonFormat[T]

  def toJson(t: T): JsValue = format.write(t)
  def fromJson(json: JsValue): T = format.read(json)

  def toBytes(t: T): Array[Byte] = format.write(t).compactPrint.getBytes("UTF-8")
  def fromBytes(bytes: Array[Byte]): T = format.read(new String(bytes, "UTF-8").parseJson)
}
