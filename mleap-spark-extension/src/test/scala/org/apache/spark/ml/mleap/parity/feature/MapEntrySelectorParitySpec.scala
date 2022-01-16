package org.apache.spark.ml.mleap.parity.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, MapType, StringType, StructType}
import org.apache.spark.ml.mleap.feature.MapEntrySelector

class MapEntrySelectorParitySpec extends SparkParityBase {

  val rows = spark.sparkContext.parallelize(Seq(
      Row(Map("a"->1.0), "a"),
      Row(Map("b"->2.0), "key_does_not_exist"),
      Row(Map("c"->3.0), "c")
    ))
  val defaultValue: Double = 42.0
  val schema = new StructType()
    .add("map", MapType(StringType, DoubleType), nullable = false)
    .add("key", StringType, nullable = false)

  override val dataset: DataFrame = spark.sqlContext.createDataFrame(rows, schema)
  override val sparkTransformer: Transformer = new MapEntrySelector[String, Double]()
    .setDefaultValue(defaultValue)
    .setInputCol("map")
    .setKeyCol("key")
    .setOutputCol("result")
    .fit(dataset)

  val expectedRows = Seq(
    Row(Map("a"->1.0), "a", 1.0),
    Row(Map("b"->2.0), "key_does_not_exist", defaultValue),
    Row(Map("c"->3.0), "c", 3.0)
  )

  describe("Spark transform") {
    def assertCorrectTransform(df: DataFrame) = {
      df.collect().zip(expectedRows).map {
        case (result: Row, expected: Row) => assert(result == expected)
        case _ => throw new RuntimeException("Should not encounter this case")
      }
      assert(df.schema == schema.copy().add("result", DoubleType, nullable = false))
    }
    it("produces the correct transform") {
      val df = sparkTransformer.transform(dataset)
      assertCorrectTransform(df)
      implicit val sbc = SparkBundleContext().withDataset(df)
      assertCorrectTransform(deserializedSparkTransformer(sparkTransformer).transform(dataset))
    }
  }
}
