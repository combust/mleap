package ml.combust.mleap.spark

import ml.combust.mleap.runtime.function.{ArraySelector, FieldSelector, Selector, UserDefinedFunction}
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.mleap.TypeConverters
import org.apache.spark.sql.{Column, DataFrame, expressions}
import org.apache.spark.sql.mleap.UserDefinedFunctionConverters._

import scala.util.Try

/**
  * Created by hollinwilkins on 10/22/16.
  */
case class SparkTransformBuilder(dataset: DataFrame) extends TransformBuilder[SparkTransformBuilder] {
  override def schema: StructType = {
    TypeConverters.mleapStructType(dataset.schema)
  }
  
  override def withOutputs(outputs: Seq[String], inputs: Selector*)
                          (udf: UserDefinedFunction): Try[SparkTransformBuilder] = ???

  override def withOutput(name: String, selectors: Selector *)
                         (udf: UserDefinedFunction): Try[SparkTransformBuilder] = Try {
    val sparkUdf: expressions.UserDefinedFunction = udf
    val sparkSelectors = selectors.map(sparkSelector)
    copy(dataset = dataset.withColumn(name, sparkUdf(sparkSelectors: _*)))
  }

  private def sparkSelector(selector: Selector): Column = selector match {
    case FieldSelector(name) => dataset.col(name)
    case ArraySelector(names @ _*) => struct(names.map(dataset.col): _*)
  }
}
