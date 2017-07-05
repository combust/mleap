package ml.combust.mleap.core.types

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 7/4/17.
  */
object TypeSpec {
  implicit def apply(dt: DataType): DataTypeSpec = DataTypeSpec(dt)
  implicit def apply(schema: StructType): SchemaSpec = SchemaSpec(schema)
}
sealed trait TypeSpec {
  def dataTypes: Seq[DataType]
}
case class DataTypeSpec(dt: DataType) extends TypeSpec {
  override val dataTypes: Seq[DataType] = Seq(dt)
}

object SchemaSpec {
  def
  apply(schema: StructType): SchemaSpec = SchemaSpec(schema.fields.map(_.dataType))
}
case class SchemaSpec(dts: Seq[DataType]) extends TypeSpec {
  override def dataTypes: Seq[DataType] = dts
}
