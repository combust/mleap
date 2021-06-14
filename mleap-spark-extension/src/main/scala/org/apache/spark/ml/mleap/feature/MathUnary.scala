package org.apache.spark.ml.mleap.feature

import ml.combust.mleap.core.feature.{MathUnaryModel, UnaryOperation}
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWriter, Identifiable, MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DoubleType, NumericType, StructField, StructType}
import org.apache.spark.sql.functions.udf

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathUnary(override val uid: String = Identifiable.randomUID("math_unary"),
                val model: MathUnaryModel)
  extends Transformer
  with HasInputCol
  with HasOutputCol
  with MLWritable {
  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @org.apache.spark.annotation.Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val unaryUdf = udf { a: Double => model(a) }

    dataset.withColumn($(outputCol), unaryUdf(dataset($(inputCol)).cast(DoubleType)))
  }

  override def copy(extra: ParamMap): Transformer =
    copyValues(new MathUnary(uid, model), extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(schema($(inputCol)).dataType.isInstanceOf[NumericType],
      s"Input column must be of type NumericType but got ${schema($(inputCol)).dataType}")
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")

    StructType(schema.fields :+ StructField($(outputCol), DoubleType))
  }

  override def write: MLWriter = new MathUnary.MathUnaryWriter(this)
}

object MathUnary extends MLReadable[MathUnary] {

  override def read: MLReader[MathUnary] = new MathUnaryReader

  override def load(path: String): MathUnary = super.load(path)

  private class MathUnaryWriter(instance: MathUnary) extends MLWriter {

    private case class Data(operation: String)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: fromTypeName, toTypeName
      val model = instance.model
      val operation = model.operation.name

      val data = Data(operation)
      val dataPath = new Path(path, "data").toString
      sparkSession
        .createDataFrame(Seq(data))
        .repartition(1)
        .write
        .parquet(dataPath)
    }
  }

  private class MathUnaryReader extends MLReader[MathUnary] {

    /** Checked against metadata when loading model */
    private val className = classOf[MathUnary].getName

    override def load(path: String): MathUnary = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString

      val data = sparkSession.read.parquet(dataPath).select("operation").head()
      val operation = data.getAs[String](0)

      val model = MathUnaryModel(UnaryOperation.forName(operation))
      val transformer = new MathUnary(metadata.uid, model)

      metadata.getAndSetParams(transformer)
      transformer
    }
  }

}
