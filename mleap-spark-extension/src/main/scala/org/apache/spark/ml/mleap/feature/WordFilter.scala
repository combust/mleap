package org.apache.spark.ml.mleap.feature

import ml.combust.mleap.core.feature.WordLengthFilterModel
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Model, Transformer}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._


/**
  * Created by mageswarand on 14/2/17.
  */

private[ml] trait WordFilterParams extends Params with HasInputCol with HasOutputCol {

  /**
    * Param for word length to be filtered (&gt;= 0).
    * @group param
    */
  final val wordLength: IntParam = new IntParam(this, "wordLength", "minimum word size that needs to be filtered(>= 3)", ParamValidators.gtEq(3))

  /** @group getParam */
  final def getWordLength: Int = $(wordLength)
}

class WordFilter(override val uid: String, val model: WordLengthFilterModel) extends Transformer
  with WordFilterParams
  with MLWritable
  with MLReadable[WordFilter]{


  def this(model: WordLengthFilterModel) = this(uid = Identifiable.randomUID("filter_words"), model = model)
  def this() = this(new WordLengthFilterModel)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setWordLength(value: Int = 3): this.type = set(wordLength, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val filterWordsUdf = udf {
      (words: Seq[String]) => model(words)
    }

    dataset.withColumn($(outputCol), filterWordsUdf(dataset($(inputCol))))
  }

  override def copy(extra: ParamMap): Transformer =  defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    require(schema($(inputCol)).dataType.isInstanceOf[ArrayType],
      s"Input column must be of type ArrayType(StringType,true) but got ${schema($(inputCol)).dataType}")
    val inputFields = schema.fields

    require(!inputFields.exists(_.name == $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")

    StructType(schema.fields :+ StructField($(outputCol), ArrayType(StringType, true)))

  }

  override def write: MLWriter = new WordFilterModelWritter(this)

  override def read: MLReader[WordFilter] = new WordFilterModelReader

  override def load(path: String): WordFilter = super.load(path)
}

class WordFilterModelWritter(instance: WordFilter) extends MLWriter {

  private case class Data(length: Int)

  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    val data = Data(instance.model.length)
    val dataPath = new Path(path, "data").toString
    sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
  }
}

//TODO: not working check!
class WordFilterModelReader  extends MLReader[WordFilter] {


  private val className = classOf[WordFilter].getName

  override def load(path: String): WordFilter = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
    val dataPath = new Path(path, "data").toString
    val data = sparkSession.read.parquet(dataPath)
      .select("length")
      .head()
    val length = data.getAs[Int](0)
    val model = new WordFilter(metadata.uid, new WordLengthFilterModel(length))
    DefaultParamsReader.getAndSetParams(model, metadata)
    model
  }
}