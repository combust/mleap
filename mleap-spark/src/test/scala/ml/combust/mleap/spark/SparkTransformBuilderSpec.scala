//package ml.combust.mleap.spark
//
//import ml.combust.mleap.core.feature.StringIndexerModel
//import ml.combust.mleap.runtime.transformer.feature.{StringIndexer, VectorAssembler}
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.scalatest.FunSpec
//import com.databricks.spark.avro._
//import SparkSupport._
//import ml.combust.mleap.runtime.transformer.Pipeline
//import org.apache.spark.sql.functions._
//
///**
//  * Created by hollinwilkins on 10/30/16.
//  */
//class SparkTransformBuilderSpec extends FunSpec {
//  val stringIndexer = StringIndexer(inputCol = "state",
//    outputCol = "state_index",
//    model = StringIndexerModel(Seq("WI", "NJ")))
//  val vectorAssembler = VectorAssembler(inputCols = Array("state_index"),
//    outputCol = "features")
//  val pipeline = Pipeline(transformers = Seq(stringIndexer, vectorAssembler))
//
//  describe("using a simple StringIndexer") {
//    it("transforms the Spark Dataframe") {
//      val spark = SparkSession.builder().
//        appName("MLeap Spark Tests").
//        master("local[2]").
//        getOrCreate()
//
//      val f = getClass.getClassLoader.getResource("datasources/lending_club_sample.avro")
//      val dataset: DataFrame = spark.sqlContext.read.avro(f.toString).select("state").filter("state == 'WI' or state == 'NJ'")
//      val transformedDataset = stringIndexer.sparkTransform(dataset).
//        groupBy("state", "state_index").agg(count("state")).
//        select("state", "state_index").
//        orderBy("state")
//      val results = transformedDataset.collect()
//
//      assert(results(0) == Row("NJ", 1.0))
//      assert(results(1) == Row("WI", 0.0))
//
//      spark.stop()
//    }
//  }
//
//  describe("using a VectorAssembler") {
//    it("transforms the Spark DataFrame") {
//      val spark = SparkSession.builder().
//        appName("MLeap Spark Tests").
//        master("local[2]").
//        getOrCreate()
//
//      val f = getClass.getClassLoader.getResource("datasources/lending_club_sample.avro")
//      val dataset: DataFrame = spark.sqlContext.read.avro(f.toString).
//        select("state", "dti").
//        filter(col("state").isin("WI", "NJ"))
//      val transformedDataset = pipeline.sparkTransform(dataset).select("features")
//      transformedDataset.collect()
//
//      spark.stop()
//    }
//  }
//}
