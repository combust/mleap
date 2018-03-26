package ml.combust.mleap.spark

import ml.combust.mleap.core.types.{StructField, StructType}
import ml.combust.mleap.runtime.frame.{FrameBuilder, Row, RowUtil}
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}
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
                          sqlContext: SQLContext) extends FrameBuilder[SparkLeapFrame] {
  override def withColumn(output: String, inputs: Selector *)
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

  override def withColumns(outputs: Seq[String], inputs: Selector*)
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

  override def select(fieldNames: Seq[String]): Try[SparkLeapFrame] = {
    for(indices <- schema.indicesOf(fieldNames: _*);
      schema2 <- schema.selectIndices(indices: _*)) yield {
      val dataset2 = dataset.map(row => row.selectIndices(indices: _*))

      copy(schema = schema2, dataset = dataset2)
    }
  }

  override def drop(names: String*): Try[SparkLeapFrame] = {
    for(indices <- schema.indicesOf(names: _*);
        schema2 <- schema.dropIndices(indices: _*)) yield {
      val dataset2 = dataset.map(row => row.dropIndices(indices: _*))

      copy(schema = schema2, dataset = dataset2)
    }
  }

  override def filter(selectors: Selector*)
                     (udf: UserDefinedFunction): Try[SparkLeapFrame] = {
    RowUtil.createRowSelectors(schema, selectors: _*)(udf).map {
      rowSelectors =>
        val dataset2 = dataset.filter(row => row.shouldFilter(rowSelectors: _*)(udf))
        copy(schema = schema, dataset = dataset2)
    }
  }

  def toSpark: DataFrame = {
    val spec = schema.fields.map(TypeConverters.mleapToSparkConverter)
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
