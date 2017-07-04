package ml.combust.mleap.spark

import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.function.{StructSelector, FieldSelector, Selector, UserDefinedFunction}
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.mleap.TypeConverters
import org.apache.spark.sql.{Column, DataFrame, expressions}
import org.apache.spark.sql.mleap.UserDefinedFunctionConverters._

import scala.util.{Random, Try}

/**
  * Created by hollinwilkins on 10/22/16.
  */
case class SparkTransformBuilder(dataset: DataFrame) extends TransformBuilder[SparkTransformBuilder] {
  override def schema: StructType = {
    TypeConverters.mleapStructType(dataset.schema)
  }

  override def withOutputs(outputs: Seq[String], inputs: Selector *)
                          (udf: UserDefinedFunction): Try[SparkTransformBuilder] = Try {
    val structUdf: expressions.UserDefinedFunction = udf
    val sparkSelectors = inputs.map(sparkSelector)
    val tmpName = s"tmp_${Random.nextInt()}"
    val dataset2 = dataset.withColumn(tmpName, structUdf(sparkSelectors: _*))
    val dataset3 = outputs.zipWithIndex.foldLeft(dataset2) {
      case (d, (name, index)) =>
        d.withColumn(name, d.col(s"$tmpName._$index"))
    }.drop(tmpName)
    copy(dataset = dataset3)
  }

  override def withOutput(name: String, selectors: Selector *)
                         (udf: UserDefinedFunction): Try[SparkTransformBuilder] = Try {
    val sparkUdf: expressions.UserDefinedFunction = udf
    val sparkSelectors = selectors.map(sparkSelector)
    copy(dataset = dataset.withColumn(name, sparkUdf(sparkSelectors: _*)))
  }

  private def sparkSelector(selector: Selector): Column = selector match {
    case FieldSelector(name) => dataset.col(name)
    case StructSelector(names @ _*) =>
      val cols = names.zipWithIndex.map {
        case (name, index) => dataset.col(name).as(s"_$index")
      }
      struct(cols: _*)
  }
}
