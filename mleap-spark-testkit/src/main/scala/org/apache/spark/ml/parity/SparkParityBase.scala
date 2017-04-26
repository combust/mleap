package org.apache.spark.ml.parity

import java.io.File

import ml.combust.mleap.runtime
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import ml.combust.mleap.spark.SparkSupport._
import ml.combust.mleap.runtime.MleapSupport._
import com.databricks.spark.avro._
import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.mleap.TensorUDT
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.ArrayType
import ml.combust.mleap.core.util.VectorConverters._
import resource._

import scala.collection.mutable

/**
  * Created by hollinwilkins on 10/30/16.
  */
object SparkParityBase extends FunSpec {
  TensorUDT // make sure UDT is registered

  val sparkRegistry = SparkBundleContext.defaultContext
  val mleapRegistry = MleapContext.defaultContext

  def textDataset(spark: SparkSession): DataFrame = {
    spark.sqlContext.read.text(getClass.getClassLoader.getResource("datasources/carroll-alice.txt").toString).
      withColumnRenamed("value", "text")
  }
  def dataset(spark: SparkSession): DataFrame = {
    spark.sqlContext.read.avro(getClass.getClassLoader.getResource("datasources/lending_club_sample.avro").toString)
  }

  val toTensor = udf {
    v: Vector => v: Tensor[Double]
  }

  val toTensorArray = udf {
    v: mutable.WrappedArray[Vector] => v.map(vv => vv: Tensor[Double])
  }
}

abstract class SparkParityBase extends FunSpec with BeforeAndAfterAll {
  val baseDataset: DataFrame = SparkParityBase.dataset(spark)
  val textDataset: DataFrame = SparkParityBase.textDataset(spark)
  val dataset: DataFrame
  val sparkTransformer: Transformer

  def sparkCols(dataset: DataFrame): Seq[Column] = {
    dataset.schema.fields.sortBy(_.name).map {
      field =>
        field.dataType match {
          case _: VectorUDT =>
            SparkParityBase.toTensor(dataset.col(field.name))
          case at: ArrayType if at.elementType.isInstanceOf[VectorUDT] =>
            SparkParityBase.toTensorArray(dataset.col(field.name))
          case _ => dataset.col(field.name)
        }
    }
  }
  def mleapCols(dataset: DataFrame): Seq[Column] = {
    dataset.schema.fieldNames.sortBy(identity).map(dataset.col)
  }

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
      val file = new File(s"/tmp/mleap/spark-parity/${getClass.getName}.zip")
      file.delete()

      for(bf <- managed(BundleFile(file))) {
        transformer.writeBundle.save(bf).get
      }

      bundleCache = Some(file)
      file
    }
  }

  def mleapTransformer(transformer: Transformer)
                      (implicit context: SparkBundleContext): runtime.transformer.Transformer = {
    (for(bf <- managed(BundleFile(serializedModel(transformer)))) yield {
      bf.loadMleapBundle().get.root
    }).tried.get
  }

  def deserializedSparkTransformer(transformer: Transformer): Transformer = {
    (for(bf <- managed(BundleFile(serializedModel(transformer)))) yield {
      bf.loadSparkBundle().get.root
    }).tried.get
  }

  def parityTransformer(): Unit = {
    it("has parity between Spark/MLeap") {
      val sparkTransformed = sparkTransformer.transform(dataset)
      implicit val sbc = SparkBundleContext().withDataset(sparkTransformed)
      val mTransformer = mleapTransformer(sparkTransformer)
      val sparkDataset = sparkTransformed.select(sparkCols(sparkTransformed): _*).collect()
      val mleapTransformed = mTransformer.sparkTransform(dataset)
      val mleapDataset = mleapTransformed.select(mleapCols(mleapTransformed): _*).collect()

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
