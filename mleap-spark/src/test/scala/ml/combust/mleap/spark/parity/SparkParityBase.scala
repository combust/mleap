package ml.combust.mleap.spark.parity

import java.io.File

import ml.combust.bundle.BundleRegistry
import ml.combust.mleap.runtime
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import ml.combust.mleap.spark.SparkSupport.{MleapTransformerOps, SparkTransformerOps}
import ml.combust.mleap.runtime.MleapSupport.FileOps
import com.databricks.spark.avro._

/**
  * Created by hollinwilkins on 10/30/16.
  */
object SparkParityBase extends FunSpec {
  val sparkRegistry = BundleRegistry("spark")
  val mleapRegistry = BundleRegistry("mleap")
  def dataset(spark: SparkSession) = spark.sqlContext.read.avro(getClass.getClassLoader.getResource("datasources/lending_club_sample.avro").toString)

  def mleapTransformer(transformer: Transformer): runtime.transformer.Transformer = {
    new File("/tmp/mleap/spark-parity").mkdirs()
    val file = new File(s"/tmp/mleap/spark-parity/${java.util.UUID.randomUUID().toString}")
    transformer.serializeToBundle(file)(sparkRegistry)
    file.deserializeBundle()(mleapRegistry)._2
  }
}

abstract class SparkParityBase extends FunSpec with BeforeAndAfterAll {
  val baseDataset: DataFrame = SparkParityBase.dataset(spark)
  val dataset: DataFrame
  val sparkTransformer: Transformer

  lazy val spark = SparkSession.builder().
    appName("Spark/MLeap Parity Tests").
    master("local[2]").
    getOrCreate()

  override protected def afterAll(): Unit = spark.stop()

  def parityTransformer(): Unit = {
    it("has parity between Spark/MLeap") {
      val mTransformer = SparkParityBase.mleapTransformer(sparkTransformer)
      val sparkDataset = sparkTransformer.transform(dataset).collect()
      val mleapDataset = mTransformer.sparkTransform(dataset).collect()

      assert(sparkDataset sameElements mleapDataset)
    }
  }

  it should behave like parityTransformer()
}
