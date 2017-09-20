package org.apache.spark.ml.parity

import java.io.File

import ml.combust.mleap.runtime
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import ml.combust.mleap.runtime.MleapSupport._
import com.databricks.spark.avro._
import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{BasicType, DataType, TensorType}
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.bundle.SparkBundleContext
import ml.combust.mleap.spark.SparkSupport._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{BaseTransformer, Pipeline}
import resource._

/**
  * Created by hollinwilkins on 10/30/16.
  */
object SparkParityBase extends FunSpec {
  val sparkRegistry = SparkBundleContext.defaultContext
  val mleapRegistry = MleapContext.defaultContext

  def textDataset(spark: SparkSession): DataFrame = {
    spark.sqlContext.read.text(getClass.getClassLoader.getResource("datasources/carroll-alice.txt").toString).
      withColumnRenamed("value", "text")
  }
  def dataset(spark: SparkSession): DataFrame = {
    spark.sqlContext.read.avro(getClass.getClassLoader.getResource("datasources/lending_club_sample.avro").toString)
  }
}

abstract class SparkParityBase extends FunSpec with BeforeAndAfterAll {
  val baseDataset: DataFrame = SparkParityBase.dataset(spark)
  val textDataset: DataFrame = SparkParityBase.textDataset(spark)
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
      val file = new File(s"/tmp/mleap/spark-parity/${getClass.getName}.zip")
      file.delete()

      for(bf <- managed(BundleFile(file))) {
        transformer.writeBundle.format(SerializationFormat.Json).save(bf).get
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

  def asssertModelTypesMatchTransformerTypes(model: Model, exec: UserDefinedFunction) = {
//    println("\n\n*************** INPUT")
//    println("*************** INPUT")
//    println("*************** INPUT")
//    println(model)
//    println("=============== INPUT")
//    println(model.inputSchema)
//    println("=============== INPUT")
//    println("MODEL: " + model.inputSchema.fields.map(field => field.dataType))
//    println("TRANSFORMER: " + exec.inputs.map(in => in.dataTypes).flatten)
//    println("===============\n\n")
    checkTypes(model.inputSchema.fields.map(field => field.dataType),
      exec.inputs.map(in => in.dataTypes).flatten)

//    println("\n\n***************")
//    println("***************")
//    println(model)
//    println("===============")
//    println(model.outputSchema)
//    println("===============")
//    println("MODEL: " + model.outputSchema.fields.map(field => field.dataType))
//    println("TRANSFORMER: " + exec.output.dataTypes)
//    println("===============\n\n")
    checkTypes(model.outputSchema.fields.map(field => field.dataType),
      exec.output.dataTypes)
  }

  def checkTypes(modelTypes: Seq[DataType], transformerTypes: Seq[DataType]) = {
    assert(modelTypes.size == modelTypes.size)
    modelTypes.zip(transformerTypes).foreach {
      case (modelType, transformerType) => {
        if (modelType.isInstanceOf[TensorType]) {
          assert(transformerType.isInstanceOf[TensorType] &&
            modelType.base == transformerType.base)
        } else {
          assert(modelType == transformerType)
        }
      }
    }
  }

  def parityTransformer(): Unit = {
    it("has parity between Spark/MLeap") {
      val sparkTransformed = sparkTransformer.transform(dataset)
      implicit val sbc = SparkBundleContext().withDataset(sparkTransformed)
      val mTransformer = mleapTransformer(sparkTransformer)
      val sparkDataset = sparkTransformed.toSparkLeapFrame.toSpark.collect()
      val mleapTransformed = mTransformer.sparkTransform(dataset)
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
              assert(v2.isDefined, s"v2 is not defined $param2")

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

    it("model input/output schema matches transformer UDF") {
      val mTransformer = mleapTransformer(sparkTransformer)

      mTransformer match {
        case transformer: BaseTransformer => {
          asssertModelTypesMatchTransformerTypes(transformer.model, transformer.exec)
        }
        case pipeline: Pipeline => {
          pipeline.transformers.foreach(tran => tran match {
            case stage: BaseTransformer => {
              asssertModelTypesMatchTransformerTypes(stage.model, stage.exec)
            }
            case _ => assert(true) // no udf to check against
          })
        }
        case _ => assert(true) // no udf to check against
      }
   }
  }

  it should behave like parityTransformer()
}
