package org.apache.spark.ml.mleap.feature

import ml.combust.mleap.core.feature.{MapEntrySelectorModel => MleapModel}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.mleap.TypeConverters.sparkTypeToMleapBasicType
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{DataType, StructType}

import scala.reflect.runtime.universe.TypeTag

trait HasMapEntrySelectorParams extends Params with HasInputCol with HasOutputCol{

  final val keyCol: Param[String] = new Param[String](
    this, "keyCol", "column containing key to select")
  final val defaultValue: Param[Any] = new Param[Any](
    this, "defaultValue", "default value to use if key is missing"
  )

  def setDefaultValue(value: Any): this.type = set(defaultValue, value)
  final def getDefaultValue: Any = $(defaultValue)
  def setKeyCol(value: String): this.type = set(keyCol, value)
  final def getKeyCol: String = $(keyCol)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
}

class MapEntrySelectorModel[K: TypeTag,V: TypeTag](
                             val keyType: DataType, val valueType: DataType, override val uid: String
                           ) extends Model[MapEntrySelectorModel[K,V]] with HasMapEntrySelectorParams{

  def this(keyType: DataType, valueType: DataType) = this(
    keyType, valueType, uid = Identifiable.randomUID("map_entry_selector"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val model = MleapModel[K,V]($(defaultValue).asInstanceOf[V])
    val mapEntrySelectorUDF = udf { (m: Map[K,V], key: K) => model(m, key) }
    dataset.withColumn($(outputCol), mapEntrySelectorUDF(dataset($(inputCol)), dataset($(keyCol))))
  }

  override def copy(extra: ParamMap): MapEntrySelectorModel[K,V] = {
    copyValues(new MapEntrySelectorModel[K,V](keyType, valueType, uid), extra)
  }

  @DeveloperApi
  override def transformSchema(schema: types.StructType): types.StructType = {
    require(
      schema($(inputCol)).dataType.isInstanceOf[types.MapType],
      s"Input column must be of type ${types.MapType} but got ${schema($(inputCol)).dataType}"
    )
    require(
      schema($(keyCol)).dataType.isInstanceOf[keyType.type],
      s"Input column must be of type ${keyType} but got ${schema($(keyCol)).dataType}"
    )
    val inputFields = schema.fields
    require(
      !inputFields.exists(_.name == $(outputCol)),
      s"Output column ${$(outputCol)} already exists."
    )
    types.StructType(schema.fields :+ types.StructField($(outputCol), valueType))
  }
}

class MapEntrySelector[K: TypeTag,V: TypeTag](override val uid: String) extends Estimator[MapEntrySelectorModel[K,V]] with HasMapEntrySelectorParams{

  def this() = this(uid = Identifiable.randomUID("map_entry_selector"))

  @org.apache.spark.annotation.Since("2.0.0")
  override def fit(dataset: Dataset[_]): MapEntrySelectorModel[K,V] = {
    val (keyType, valueType) = dataset.schema($(inputCol)).dataType match {
      case types.MapType(kType: types.DataType, vType: types.DataType, _) =>
        (kType, vType)
      case o => throw new ClassCastException(s"Cannot cast $o to MapType")
    }
    new MapEntrySelectorModel[K,V](keyType, valueType, uid)
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setKeyCol($(keyCol))
      .setDefaultValue($(defaultValue))
  }

  override def copy(extra: ParamMap): MapEntrySelector[K,V] = {
    copyValues(new MapEntrySelector[K,V](uid), extra)
  }

  override def transformSchema(schema: StructType): StructType = schema
}
