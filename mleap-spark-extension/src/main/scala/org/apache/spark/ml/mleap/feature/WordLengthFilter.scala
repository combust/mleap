package org.apache.spark.ml.mleap.feature

import ml.combust.mleap.core.feature.WordLengthFilterModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}


/**
  * Created by mageswarand on 14/2/17.
  */
private[ml] trait WordLengthFilterParams extends Params with HasInputCol with HasOutputCol {

  /**
    * Param for word length to be filtered (&gt;= 0).
    * @group param
    */
  final val wordLength: IntParam = new IntParam(this, "wordLength", "minimum word size that needs to be filtered(>= 3)", ParamValidators.gtEq(3))

  /** @group getParam */
  final def getWordLength: Int = $(wordLength)
}

class WordLengthFilter(override val uid: String) extends Transformer
  with WordLengthFilterParams
  with DefaultParamsWritable {

  val defaultLength = 3
  var model: WordLengthFilterModel = new WordLengthFilterModel(defaultLength) //Initialize with default filter length 3

  def this(model: WordLengthFilterModel) = this(uid = Identifiable.randomUID("filter_words"))
  def this() = this(new WordLengthFilterModel)

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setWordLength(value: Int = defaultLength): this.type = set(wordLength, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    if(defaultLength != getWordLength) model = new WordLengthFilterModel(getWordLength)
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
}

object WordLengthFilter extends  DefaultParamsReadable[WordLengthFilter] {
  override def load(path: String): WordLengthFilter = super.load(path)
}
