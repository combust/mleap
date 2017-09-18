package ml.combust.mleap.spark

import ml.combust.mleap.core.types.{StructField, StructType}
import ml.combust.mleap.runtime.{Row, RowUtil}
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.mleap.TypeConverters
import org.apache.spark.sql.{DataFrame, SQLContext, types}

import scala.util.Try

/**
  * Created by hollinwilkins on 9/1/17.
  */
case class SparkLeapFrame(schema: StructType,
                          dataset: RDD[Row],
                          sqlContext: SQLContext) extends TransformBuilder[SparkLeapFrame] {
  override def withOutput(output: String, inputs: Selector *)
                         (udf: UserDefinedFunction): Try[SparkLeapFrame] = {
    RowUtil.createRowSelectors(schema, inputs: _*)(udf).flatMap {
      rowSelectors =>
        val field = StructField(output, udf.outputTypes.head)

        schema.withField(field).map {
          schema2 =>
            val dataset2 = dataset.map {
              row => row.withValue(rowSelectors: _*)(udf)
            }
            copy(schema = schema2, dataset = dataset2)
        }
    }
  }

  override def withOutputs(outputs: Seq[String], inputs: Selector*)
                          (udf: UserDefinedFunction): Try[SparkLeapFrame] = {
    RowUtil.createRowSelectors(schema, inputs: _*)(udf).flatMap {
      rowSelectors =>
        val fields = outputs.zip(udf.outputTypes).map {
          case (name, dt) => StructField(name, dt)
        }

        schema.withFields(fields).map {
          schema2 =>
            val dataset2 = dataset.map {
              row => row.withValues(rowSelectors: _*)(udf)
            }
            copy(schema = schema2, dataset = dataset2)
        }
    }
  }

  def toSpark: DataFrame = {
    val spec = schema.fields.map(TypeConverters.mleapFieldToSparkField)
    val fields = spec.map(_._1)
    val converters = spec.map(_._2)
    val sparkSchema = new types.StructType(fields.toArray)
    val data = dataset.map {
      r =>
        val values = r.zip(converters).map {
          case (v, c) => c(v)
        }
        sql.Row(values.toSeq: _*)
    }

    sqlContext.createDataFrame(data, sparkSchema)
  }
}
