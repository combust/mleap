package org.apache.spark.ml.parity

import java.io.File

import ml.combust.mleap.runtime
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import ml.combust.mleap.spark.SparkSupport._
import ml.combust.mleap.runtime.MleapSupport._
import com.databricks.spark.avro._
import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.FileUtil
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.sql.functions.col
import resource._

import scala.util.Try

/**
  * Created by hollinwilkins on 10/30/16.
  */
object SparkParityBase extends FunSpec {
  val sparkRegistry = SparkBundleContext.defaultContext
  val mleapRegistry = MleapContext.defaultContext

  def dataset(spark: SparkSession): DataFrame = {
    spark.sqlContext.read.avro(getClass.getClassLoader.getResource("datasources/lending_club_sample.avro").toString)
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

  var bundleCache: Option[File] = None

  def serializedModel(transformer: Transformer)
                     (implicit context: SparkBundleContext): File = {
    bundleCache.getOrElse {
      new File("/tmp/mleap/spark-parity").mkdirs()
      val file = new File(s"/tmp/mleap/spark-parity/${getClass.getName}")

      BundleFile(file)
      for(bf <- managed(BundleFile(file))) {
        transformer.writeBundle.force(true).save(bf).get
      }

      bundleCache = Some(file)
      file
    }
  }

  def mleapTransformer(transformer: Transformer)
                      (implicit context: SparkBundleContext): runtime.transformer.Transformer = {
    MleapBundleFileOps(serializedModel(transformer)).loadBundle().get.root
  }

  def deserializedSparkTransformer(transformer: Transformer): Transformer = {
    SparkBundleFileOps(serializedModel(transformer)).loadBundle().get.root
  }

  def parityTransformer(): Unit = {
    it("has parity between Spark/MLeap") {
      val sparkTransformed = sparkTransformer.transform(dataset)
      implicit val sbc = SparkBundleContext().withDataset(sparkTransformed)
      val mTransformer = mleapTransformer(sparkTransformer)
      val fields = sparkTransformed.schema.fields.map(_.name).map(col)
      val sparkDataset = sparkTransformed.select(fields: _*).collect()
      val mleapTransformed = mTransformer.sparkTransform(dataset).select(fields: _*)
      val mleapDataset = mleapTransformed.collect()

      assert(sparkDataset sameElements mleapDataset)
    }

    it("serializes/deserializes the Spark model properly") {
      val deserializedSparkModel = deserializedSparkTransformer(sparkTransformer)
      sparkTransformer.params.zip(deserializedSparkModel.params).foreach {
        case (param1, param2) =>
          val v1 = sparkTransformer.get(param1)
          val v2 = deserializedSparkModel.get(param2)

          v1 match {
            case Some(v1Value) =>
              v1Value match {
                case v1Value: Array[_] =>
                  assert(v1Value sameElements v2.get.asInstanceOf[Array[_]])
                case _ =>
                  assert(v1Value == v2.get)
              }
            case None =>
              assert(v2.isEmpty)
          }
      }
    }
  }

  it should behave like parityTransformer()
}
