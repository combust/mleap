package org.apache.spark.ml.parity

import java.io.File
import java.nio.file.{Files, Path}

import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{DataType, NodeShape, TensorType}
import ml.combust.mleap.runtime.frame.{BaseTransformer, MultiTransformer, SimpleTransformer}
import ml.combust.mleap.runtime.{MleapContext, frame}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import org.apache.spark.ml.bundle.SparkBundleContext
import ml.combust.mleap.spark.SparkSupport._
import ml.combust.mleap.runtime.transformer.Pipeline
import resource._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.TestingUtils._

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
    spark.sqlContext.read.format("avro").load(getClass.getClassLoader.getResource("datasources/lending_club_sample.avro").toString)
  }

  def multiClassClassificationDataset(spark: SparkSession): DataFrame = {
    spark.sqlContext.read.format("libsvm").load(getClass.getClassLoader.getResource("datasources/sample_multiclass_classification_data.txt").toString)
  }

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def recommendationDataset(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.read.textFile(this.getClass.getClassLoader.getResource("datasources/sample_movielens_ratings.txt").toString)
                         .map(parseRating)
                         .toDF()
  }
}

abstract class SparkParityBase extends FunSpec with BeforeAndAfterAll {
  lazy val baseDataset: DataFrame = SparkParityBase.dataset(spark)
  lazy val textDataset: DataFrame = SparkParityBase.textDataset(spark)
  lazy val recommendationDataset: DataFrame = SparkParityBase.recommendationDataset(spark)
  lazy val multiClassClassificationDataset: DataFrame = SparkParityBase.multiClassClassificationDataset(spark)

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

      val tempDirPath = {
        val temp: Path = Files.createTempDirectory("mleap-spark-parity")
        temp.toFile.deleteOnExit()
        temp.toAbsolutePath
      }

      val file = new File(s"${tempDirPath}/${getClass.getName}.zip")

      for(bf <- managed(BundleFile(file))) {
        transformer.writeBundle.format(SerializationFormat.Json).save(bf).get
      }

      bundleCache = Some(file)
      file
    }
  }

  def mleapTransformer(transformer: Transformer)
                      (implicit context: SparkBundleContext): frame.Transformer = {
    (for(bf <- managed(BundleFile(serializedModel(transformer)))) yield {
      bf.loadMleapBundle().get.root
    }).tried.get
  }

  def deserializedSparkTransformer(transformer: Transformer)
                                  (implicit context: SparkBundleContext): Transformer = {
    (for(bf <- managed(BundleFile(serializedModel(transformer)))) yield {
      bf.loadSparkBundle().get.root
    }).tried.get
  }

  def assertModelTypesMatchTransformerTypes(model: Model, shape: NodeShape, exec: UserDefinedFunction): Unit = {
    val inputFields = model.inputSchema.fields.map(_.name)
    val modelInputTypes = model.inputSchema.fields.map(_.dataType)
    val transformerInputTypes = exec.inputs.flatMap(_.dataTypes)

    val outputFields = model.outputSchema.fields.map(_.name)
    val modelOutputTypes = model.outputSchema.fields.map(_.dataType)
    val transformerOutputTypes = exec.outputTypes

    checkTypes(modelInputTypes, transformerInputTypes, inputFields)
    checkTypes(modelOutputTypes, transformerOutputTypes, outputFields)
  }

  def checkTypes(modelTypes: Seq[DataType], transformerTypes: Seq[DataType], fields: Seq[String]): Unit = {
    assert(modelTypes.size == modelTypes.size)
    modelTypes.zip(transformerTypes).zip(fields).foreach {
      case ((modelType, transformerType), field) => {
        if (modelType.isInstanceOf[TensorType]) {
          assert(
            transformerType.isInstanceOf[TensorType] && modelType.base == transformerType.base,
            s"Type of ${field} does not match, $transformerType")
        } else {
          assert(modelType == transformerType, s"Type of ${field} does not match")
        }
      }
    }
  }

  def checkRowWithRelTol(actualAnswer: Row, expectedAnswer: Row, eps: Double): Unit = {
    assert(actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")
    var rowIdx = 0
    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        assert(actual ~== expected relTol eps)
        rowIdx += 1
      case (actual: Float, expected: Float) =>
        assert(actual ~== expected relTol eps)
        rowIdx += 1
      case (actual: Vector, expected: Vector) =>
        assert(actual ~= expected relTol eps)
        rowIdx += 1
      case (actual: Seq[_], expected: Seq[_]) =>
        assert(actual.length == expected.length, s"actual length ${actual.length} != " +
          s"expected length ${expected.length}")
        actual.zip(expected).foreach {
          case (actualElem: Double, expectedElem: Double) =>
            assert(actualElem ~== expectedElem relTol eps)
          case (actualElem: Float, expectedElem: Float) =>
            assert(actualElem ~== expectedElem relTol eps)
          case (actualElem, expectedElem) =>
            assert(actualElem == expectedElem, s"Field ${actualAnswer.schema(rowIdx)} differs.")
        }
        rowIdx += 1
      case (actual: Row, expected: Row) =>
        checkRowWithRelTol(actual, expected, eps)
        rowIdx += 1
      case (actual, expected) =>
        assert(actual == expected, s"$actual did not equal $expected")
        rowIdx += 1
    }
  }

  var relTolEps: Double = 1E-7
  def equalityTest(sparkDataset: DataFrame, mleapDataset: DataFrame): Unit = {
    val sparkCols = sparkDataset.columns.toSeq
    assert(mleapDataset.columns.toSet === sparkCols.toSet)
    val sparkRows = sparkDataset.collect()
    val mleapRows = mleapDataset.select(sparkCols.map(col): _*).collect()
    for ((sparkRow, mleapRow) <- sparkRows.zip(mleapRows)) {
      checkRowWithRelTol(sparkRow, mleapRow, relTolEps)
    }
  }

  def checkParamsEquality(original: Transformer, deserialized: Transformer, additionalIgnore: Set[String]): Unit = {
    val ignoredParams = unserializedParams.union(additionalIgnore)
    assert(original.params.length == deserialized.params.length)
    original.params.zip(deserialized.params).foreach {
      case (param1, param2) => if(!ignoredParams.contains(param1.name)) {
        assert(original.isDefined(param1) == deserialized.isDefined(param2),
          s"spark transformer param ${param1.name} is defined ${original.isDefined(param1)} deserialized is ${deserialized.isDefined(param2)}")

        if (original.isDefined(param1)) {
          val v1Value = original.getOrDefault(param1)
          val v2Value = deserialized.getOrDefault(param2)

          v1Value match {
            case v1Value: Array[_] => assert(v1Value sameElements v2Value.asInstanceOf[Array[_]])
            case _ => assert(v1Value == v2Value, s"$param1 is not equivalent")
          }
        }
      }
    }
  }

  def checkEquality(original: Transformer, deserialized: Transformer, additionalIgnoreParams: Set[String]): Unit = {
    assert(original.getClass == deserialized.getClass)
    assert(original.uid == deserialized.uid)
    checkParamsEquality(original, deserialized, additionalIgnoreParams)
    original match {
      case original: PipelineModel =>
        val deStages = deserialized.asInstanceOf[PipelineModel].stages
        assert(original.stages.length == deStages.length)
        original.stages.zip(deStages).foreach {
          case (o, d) => checkEquality(o, d, additionalIgnoreParams)
        }
      case _ =>
    }
  }

  def parityTransformer(): Unit = {
    it("has parity between Spark/MLeap") {
      val sparkTransformed = sparkTransformer.transform(dataset)
      implicit val sbc = SparkBundleContext().withDataset(sparkTransformed)
      val mTransformer = mleapTransformer(sparkTransformer)
      val sparkDataset = sparkTransformed.toSparkLeapFrame.toSpark
      val mleapDataset = mTransformer.sparkTransform(dataset)

      equalityTest(sparkDataset, mleapDataset)
    }

    it("serializes/deserializes the Spark model properly") {
      if (!ignoreSerializationTest) {
        checkEquality(sparkTransformer, deserializedSparkTransformer(sparkTransformer), Set())
      }
    }

    it("model input/output schema matches transformer UDF") {
      val mTransformer = mleapTransformer(sparkTransformer)
      mTransformer match {
        case transformer: SimpleTransformer =>
          assertModelTypesMatchTransformerTypes(transformer.model, transformer.shape, transformer.typedExec)
        case transformer: MultiTransformer =>
          assertModelTypesMatchTransformerTypes(transformer.model, transformer.shape, transformer.typedExec)
        case transformer: BaseTransformer =>
          assertModelTypesMatchTransformerTypes(transformer.model, transformer.shape, transformer.exec)
        case pipeline: Pipeline =>
          pipeline.transformers.foreach {
            case transformer: SimpleTransformer =>
              assertModelTypesMatchTransformerTypes(transformer.model, transformer.shape, transformer.typedExec)
            case transformer: MultiTransformer =>
              assertModelTypesMatchTransformerTypes(transformer.model, transformer.shape, transformer.typedExec)
            case stage: BaseTransformer =>
              assertModelTypesMatchTransformerTypes(stage.model, stage.shape, stage.exec)
            case _ => // no udf to check against
          }
        case _ => // no udf to check against
      }
   }
  }

  /**
    * Params that are only relevant during training and are not serialized
    */
  protected val unserializedParams: Set[String] = Set.empty

  /**
    * Can be set to true for models that are not serialized
    */
  protected val ignoreSerializationTest: Boolean = false

  it should behave like parityTransformer()
}
