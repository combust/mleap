package org.apache.spark.ml.mleap.feature

import ml.combust.mleap.core.feature.StringMapModel
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{
  DefaultParamsReadable,
  DefaultParamsWritable,
  Identifiable
}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._

/**
  * Created by hollinwilkins on 2/5/17.
  */
class StringMap(override val uid: String, val model: StringMapModel)
    extends Transformer
    with HasInputCol
    with HasOutputCol
    with DefaultParamsWritable {
  def this(model: StringMapModel) =
    this(uid = Identifiable.randomUID("string_map"), model = model)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @org.apache.spark.annotation.Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val stringMapUdf = udf { (label: String) =>
      model(label)
    }

    dataset.withColumn($(outputCol), stringMapUdf(dataset($(inputCol))))
  }

  override def copy(extra: ParamMap): Transformer =
    copyValues(new StringMap(uid, model), extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(
      schema($(inputCol)).dataType.isInstanceOf[StringType],
      s"Input column must be of type StringType but got ${schema($(inputCol)).dataType}"
    )
    val inputFields = schema.fields
    require(
      !inputFields.exists(_.name == $(outputCol)),
      s"Output column ${$(outputCol)} already exists."
    )

    StructType(schema.fields :+ StructField($(outputCol), DoubleType))
  }
}

object StringMap extends DefaultParamsReadable[StringMap] {
  override def load(path: String): StringMap = super.load(path)
}
