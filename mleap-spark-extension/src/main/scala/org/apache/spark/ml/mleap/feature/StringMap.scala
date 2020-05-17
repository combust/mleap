package org.apache.spark.ml.mleap.feature

import ml.combust.mleap.core.feature.{HandleInvalid, StringMapModel}
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
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
    with MLWritable {
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

  override def write: MLWriter = new StringMap.StringMapWriter(this)

}

object StringMap extends MLReadable[StringMap] {

  override def read: MLReader[StringMap] = new StringMapReader

  override def load(path: String): StringMap = super.load(path)

  private class StringMapWriter(instance: StringMap) extends MLWriter {

    private case class Data(labels: Map[String, Double], handleInvalid: String, defaultValue: Double)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: fromTypeName, toTypeName
      val model = instance.model
      val labels = model.labels
      val handleInvalid = model.handleInvalid.asParamString
      val defaultValue = model.defaultValue

      val data = Data(labels, handleInvalid, defaultValue)
      val dataPath = new Path(path, "data").toString
      sparkSession
        .createDataFrame(Seq(data))
        .repartition(1)
        .write
        .parquet(dataPath)
    }
  }

  private class StringMapReader extends MLReader[StringMap] {

    /** Checked against metadata when loading model */
    private val className = classOf[StringMap].getName

    override def load(path: String): StringMap = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString

      val data = sparkSession.read.parquet(dataPath).select("labels", "handleInvalid", "defaultValue").head()
      val labels = data.getAs[Map[String, Double]](0)
      val handleInvalid = HandleInvalid.fromString(data.getAs[String](1))
      val defaultValue = data.getAs[Double](2)

      val model = new StringMapModel(labels, handleInvalid = handleInvalid, defaultValue = defaultValue)
      val transformer = new StringMap(metadata.uid, model)

      metadata.getAndSetParams(transformer)
      transformer
    }
  }

}
