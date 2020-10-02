package ml.dmlc.xgboost4j.scala.spark.mleap

import java.io.File
import java.nio.file.{Files, Path}

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.runtime.frame
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.mleap.SparkUtil
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.mleap.TypeConverters
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import resource.managed

import scala.collection.mutable

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class PowerPlantTableForClassifier(AT: Double, V : Double, AP : Double, RH : Double, PE : Int)

class XGBoostClassificationModelParitySpec extends FunSpec
  with BeforeAndAfterAll {

  val spark = SparkSession.builder().
    master("local[2]").
    appName("XGBoostRegressionModelParitySpec").
    getOrCreate()

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  private val xgboostParams: Map[String, Any] = Map(
    "eta" -> 0.3,
    "max_depth" -> 2,
    "objective" -> "binary:logistic",
    "early_stopping_rounds" ->2,
    "num_round" -> 15,
    "num_classes" -> 2,
    "nworkers" -> 2
  )

  private val denseDataset: DataFrame = {
    SparkParityBase.dataset(spark).select("fico_score_group_fnl", "dti").
      filter(col("fico_score_group_fnl") === "500 - 550" ||
        col("fico_score_group_fnl") === "600 - 650")
  }

  private val sparseDataset: DataFrame = {
    import spark.sqlContext.implicits._

    spark.sqlContext.sparkContext.textFile(this.getClass.getClassLoader.getResource("datasources/xgboost_training.csv").toString)
      .map(x => x.split(","))
      .map(line => PowerPlantTableForClassifier(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble.toInt % 2))
      .toDF
  }

  private val featurePipelineForDenseDataset: Transformer = {
    new Pipeline().setStages(Array(new StringIndexer().
      setInputCol("fico_score_group_fnl").
      setOutputCol("fico_index"),
      new VectorAssembler().
        setInputCols(Array("dti")).
        setOutputCol("features"))).fit(denseDataset)
  }

  private val sparkTransformerForSparseDataset: Transformer = {
    val featureAssembler = new VectorAssembler()
      .setInputCols(Array("AT", "V", "AP", "RH"))
      .setOutputCol("features")

    val params = xgboostParams + ("missing"-> 0.0f)
    val classifier = createClassifier(params, featureAssembler, sparseDataset, "PE")
    SparkUtil.createPipelineModel(Array(featureAssembler, classifier))
  }

  private val sparkTransformerForDenseDataset: Transformer = {

    val classifier = createClassifier(xgboostParams, featurePipelineForDenseDataset, denseDataset, "fico_index")
    SparkUtil.createPipelineModel(Array(featurePipelineForDenseDataset, classifier))
  }

  def createClassifier(
                        xgboostParams: Map[String, Any],
                        featurePipeline: Transformer,
                        dataset: DataFrame,
                        outputCol: String
                      ): Transformer ={
    new XGBoostClassifier(xgboostParams).
      setFeaturesCol("features").
      setProbabilityCol("probabilities").
      setLabelCol(outputCol).
      fit(featurePipeline.transform(dataset)).
      setLeafPredictionCol("leaf_prediction").
      setContribPredictionCol("contrib_prediction").
      setTreeLimit(2)
  }



  def equalityTest(sparkDataset: DataFrame,
                   mleapDataset: DefaultLeapFrame): Unit = {
    val sparkFeaturesCol = sparkDataset.schema.fieldIndex("features")
    val mleapFeaturesCol = mleapDataset.schema.indexOf("features").get

    val sparkProbabilityCol = sparkDataset.schema.fieldIndex("probabilities")
    val mleapProbabilityCol = mleapDataset.schema.indexOf("probabilities").get

    val sparkPredictionCol = sparkDataset.schema.fieldIndex("prediction")
    val mleapPredictionCol = mleapDataset.schema.indexOf("prediction").get

    val sparkLeafPredictionCol = sparkDataset.schema.fieldIndex("leaf_prediction")
    val mleapLeafPredictionCol = mleapDataset.schema.indexOf("leaf_prediction").get

    val sparkContribPredictionCol = sparkDataset.schema.fieldIndex("contrib_prediction")
    val mleapContribPredictionCol = mleapDataset.schema.indexOf("contrib_prediction").get

    assert(sparkDataset.schema.fields.length == mleapDataset.schema.fields.length)

    sparkDataset.collect().zip(mleapDataset.collect()).foreach {
      case (sp, ml) =>
        assert(sp.getAs[Vector](sparkFeaturesCol).toDense.values sameElements ml.getTensor[Double](mleapFeaturesCol).toDense.rawValues)

        val sparkProbabilities = sp.getAs[Vector](sparkProbabilityCol).toArray
        val mleapProbabilities = ml.getTensor[Double](mleapProbabilityCol).toArray

        sparkProbabilities.zip(mleapProbabilities).foreach {
          case (v1, v2) =>
            if (Math.abs(v2 - v1) > 0.0000001) {
              println("SPARK: " + sparkProbabilities.mkString(","))
              println("MLEAP: " + mleapProbabilities.mkString(","))
            }

            assert(Math.abs(v2 - v1) < 0.0000001)
        }
        val sparkPrediction = sp.getDouble(sparkPredictionCol)
        val mleapPrediction = ml.getDouble(mleapPredictionCol)
        assert(sparkPrediction == mleapPrediction)

        val sparkLeafPrediction = sp.getAs[mutable.WrappedArray[Double]](sparkLeafPredictionCol)
        val mleapLeafPrediction = ml.getSeq[Double](mleapLeafPredictionCol)
        assert(sparkLeafPrediction == mleapLeafPrediction)

        val sparkContribPrediction = sp.getAs[mutable.WrappedArray[Double]](sparkContribPredictionCol)
        val mleapContribPrediction = ml.getSeq[Double](mleapContribPredictionCol)
        assert(sparkContribPrediction == mleapContribPrediction)
    }
  }

  var bundleCacheSparse : Option[File] = None
  var bundleCacheDense : Option[File] = None

  def serializeModelToMleapBundle(transformer: Transformer, dataset: DataFrame): File = {
    import ml.combust.mleap.spark.SparkSupport._

    implicit val sbc = SparkBundleContext.defaultContext.withDataset(transformer.transform(dataset))

    val tempDirPath = {
      val temp: Path = Files.createTempDirectory("mleap-spark-parity")
      temp.toFile.deleteOnExit()
      temp.toAbsolutePath
    }

    val file = new File(s"${tempDirPath}/${classOf[XGBoostClassificationModelParitySpec].getName}.zip")
    file.delete()

    for(bf <- managed(BundleFile(file))) {
      transformer.writeBundle.format(SerializationFormat.Json).save(bf).get
    }
    file
  }

  def loadMleapTransformerFromBundle(bundleFile: File)
                                    (implicit context: SparkBundleContext): frame.Transformer = {
    import ml.combust.mleap.runtime.MleapSupport._
    (for(bf <- managed(BundleFile(bundleFile))) yield {
      bf.loadMleapBundle().get.root
    }).tried.get
  }

  def constructLeapFrameFromSparkDataFrame(dataFrame: DataFrame): DefaultLeapFrame ={
    val mleapSchema = TypeConverters.sparkSchemaToMleapSchema(dataFrame)
    val data = dataFrame.collect().map {
      r => Row(r.toSeq: _*)
    }
    DefaultLeapFrame(mleapSchema, data)
  }

  def doTest(sparkTransformer: Transformer, dataset: DataFrame, bundleCache: Option[File]): Unit ={
    val sparkDataset = sparkTransformer.transform(dataset)
    val leapFrame = constructLeapFrameFromSparkDataFrame(dataset)
    val mleapBundle = bundleCache.getOrElse(serializeModelToMleapBundle(sparkTransformer, dataset))
    val mleapTransformer = loadMleapTransformerFromBundle(mleapBundle)
    val mleapDataset = mleapTransformer.transform(leapFrame).get

    equalityTest(sparkDataset, mleapDataset)
  }

  it("produces the same results for a dense dataset") {
    doTest(sparkTransformerForDenseDataset, denseDataset, bundleCacheDense)
  }

  it("produces the same result for a sparse dataset") {
    doTest(sparkTransformerForSparseDataset, sparseDataset, bundleCacheSparse)
  }
}
