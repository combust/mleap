package org.apache.spark.ml.mleap.feature

import ml.combust.mleap.core.feature.WordLengthFilterModel
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.ml.bundle.ops.feature.TokenizerOp


/**
  * Created by mageswarand on 14/2/17.
  * import org.apache.spark.ml.feature.Tokenizer
  * https://github.com/combust/mleap/blob/master/mleap-spark-extension/src/main/scala/org/apache/spark/ml/mleap/feature/StringMap.scala
  * https://github.com/combust/mleap/blob/master/mleap-spark-extension/src/main/scala/org/apache/spark/ml/mleap/feature/OneHotEncoder.scala
  */

private[ml] trait HasLength extends Params {

  /**
    * Param for word length to be filtered (&gt;= 0).
    * @group param
    */
  final val wordLength: IntParam = new IntParam(this, "wordLength", "minimum word size that needs to be filtered(>= 3)", ParamValidators.gtEq(3))

  /** @group getParam */
  final def getWordLength: Int = $(wordLength)
}

class WordFilter(override val uid: String, val model: WordLengthFilterModel) extends Transformer
  with HasInputCol
  with HasOutputCol
  with HasLength{


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

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(schema($(inputCol)).dataType.isInstanceOf[ArrayType],
      s"Input column must be of type ArrayType(StringType,true) but got ${schema($(inputCol)).dataType}")
    val inputFields = schema.fields

    require(!inputFields.exists(_.name == $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")

    StructType(schema.fields :+ StructField($(outputCol), ArrayType(StringType, true)))

  }
}
