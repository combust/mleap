package org.apache.spark.ml.mleap.feature

import ml.combust.mleap.core.feature.MathUnaryModel
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DoubleType, NumericType, StructField, StructType}
import org.apache.spark.sql.functions.udf

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathUnary(override val uid: String = Identifiable.randomUID("math_unary"),
                val model: MathUnaryModel) extends Transformer
  with HasInputCol
  with HasOutputCol {
  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @org.apache.spark.annotation.Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val unaryUdf = udf {
      (a: Double) => model(a)
    }

    dataset.withColumn($(outputCol), unaryUdf(dataset($(inputCol)).cast(DoubleType)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(schema($(inputCol)).dataType.isInstanceOf[NumericType],
      s"Input column must be of type NumericType but got ${schema($(inputCol)).dataType}")
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")

    StructType(schema.fields :+ StructField($(outputCol), DoubleType))
  }
}
