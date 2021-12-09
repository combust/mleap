package ml.combust.mleap.core.feature

import scala.reflect.runtime.universe.TypeTag
import ml.combust.mleap.core.reflection.MleapReflection.dataType
import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{MapType, ScalarType, StructField, StructType}

case class MapEntrySelectorModel[K: TypeTag, V: TypeTag](defaultValue: V = None) extends Model {

  def apply(m: Map[K, V], key: K): V = {
    m.getOrElse(key, defaultValue.asInstanceOf[V])
  }

  override def inputSchema: StructType = StructType(
    "input" -> MapType(dataType[K].base, dataType[V].base),
    "key" -> ScalarType(dataType[K].base).nonNullable
  ).get

  override def outputSchema: StructType = StructType(StructField("output", dataType[V])).get
}
