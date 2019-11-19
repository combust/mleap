package ml.dmlc.xgboost4j.scala.spark.mleap

import java.io.File

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

  val dataset: DataFrame = {
    SparkParityBase.dataset(spark).select("fico_score_group_fnl", "dti").
      filter(col("fico_score_group_fnl") === "500 - 550" ||
        col("fico_score_group_fnl") === "600 - 650")
  }

  val sparkTransformer: Transformer = {
    val featurePipeline = new Pipeline().setStages(Array(new StringIndexer().
      setInputCol("fico_score_group_fnl").
      setOutputCol("fico_index"),
      new VectorAssembler().
        setInputCols(Array("dti")).
        setOutputCol("features"))).fit(dataset)

    val classifier = new XGBoostClassifier(xgboostParams).
      setFeaturesCol("features").
      setProbabilityCol("probabilities").
      setLabelCol("fico_index").
      fit(featurePipeline.transform(dataset)).
      setLeafPredictionCol("leaf_prediction").
      setContribPredictionCol("contrib_prediction").
      setTreeLimit(2)

    SparkUtil.createPipelineModel(Array(featurePipeline, classifier))
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
        assert(sp.getAs[Vector](sparkFeaturesCol).toDense.values sameElements ml.getTensor[Double](mleapFeaturesCol).rawValues)

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

  var bundleCache: Option[File] = None

  def serializedModel(transformer: Transformer): File = {
    import ml.combust.mleap.spark.SparkSupport._

    implicit val sbc = SparkBundleContext.defaultContext.withDataset(transformer.transform(dataset))

    bundleCache.getOrElse {
      new File("/tmp/mleap/spark-parity").mkdirs()
      val file = new File(s"/tmp/mleap/spark-parity/${classOf[XGBoostRegressionModelParitySpec].getName}.zip")
      file.delete()

      for(bf <- managed(BundleFile(file))) {
        transformer.writeBundle.format(SerializationFormat.Json).save(bf).get
      }

      bundleCache = Some(file)
      file
    }
  }

  def mleapTransformer(transformer: Transformer)
                      (implicit context: SparkBundleContext): frame.Transformer = {
    import ml.combust.mleap.runtime.MleapSupport._

    (for(bf <- managed(BundleFile(serializedModel(transformer)))) yield {
      bf.loadMleapBundle().get.root
    }).tried.get
  }

  it("produces the same results") {
    val sparkDataset = sparkTransformer.transform(dataset)
    val mleapSchema = TypeConverters.sparkSchemaToMleapSchema(dataset)

    val data = dataset.collect().map {
      r => Row(r.toSeq: _*)
    }
    val frame = DefaultLeapFrame(mleapSchema, data)
    val mleapT = mleapTransformer(sparkTransformer)
    val mleapDataset = mleapT.transform(frame).get

    equalityTest(sparkDataset, mleapDataset)
  }
}
