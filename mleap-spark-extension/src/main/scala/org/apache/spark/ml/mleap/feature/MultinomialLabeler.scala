package org.apache.spark.ml.mleap.feature

import ml.combust.mleap.core.feature.MultinomialLabelerModel
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.mleap.param.{HasLabelsCol, HasProbabilitiesCol}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasFeaturesCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, col}
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hollinwilkins on 1/18/17.
  */
class MultinomialLabeler(override val uid: String = Identifiable.randomUID("math_unary"),
                         val model: MultinomialLabelerModel) extends Transformer
  with HasFeaturesCol
  with HasProbabilitiesCol
  with HasLabelsCol {

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)
  def setProbabilitiesCol(value: String): this.type = set(probabilitiesCol, value)
  def setLabelsCol(value: String): this.type = set(labelsCol, value)

  @org.apache.spark.annotation.Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val probabilitiesUdf = udf {
      (vector: Vector) => model.top(vector).map(_._1).toArray
    }

    val labelsUdf = udf {
      (vector: Vector) => model.topLabels(vector).toArray
    }

    dataset.withColumn($(probabilitiesCol), probabilitiesUdf(col($(featuresCol)))).
      withColumn($(labelsCol), labelsUdf(col($(featuresCol))))
  }

  override def copy(extra: ParamMap): Transformer =
    copyValues(new MultinomialLabeler(uid, model), extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(schema($(featuresCol)).dataType.isInstanceOf[VectorUDT],
      s"Features column must be of type NumericType but got ${schema($(featuresCol)).dataType}")
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == $(probabilitiesCol)),
      s"Output column ${$(probabilitiesCol)} already exists.")
    require(!inputFields.exists(_.name == $(labelsCol)),
      s"Output column ${$(labelsCol)} already exists.")

    StructType(schema.fields ++ Seq(StructField($(probabilitiesCol), ArrayType(DoubleType)),
      StructField($(labelsCol), ArrayType(StringType))))
  }
}
