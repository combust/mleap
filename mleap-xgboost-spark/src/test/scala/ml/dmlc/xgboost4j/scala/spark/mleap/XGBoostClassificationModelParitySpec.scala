package ml.dmlc.xgboost4j.scala.spark.mleap

import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.mleap.SparkUtil
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class PowerPlantTableForClassifier(AT: Double, V : Double, AP : Double, RH : Double, PE : Int)

class XGBoostClassificationModelParitySpec extends SparkParityBase {

  val dataset: DataFrame = {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    sqlContext.sparkContext.textFile(this.getClass.getClassLoader.getResource("datasources/xgboost_training.csv").toString)
      .map(x => x.split(","))
      .map(line => PowerPlantTableForClassifier(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble.toInt % 2))
      .toDF
  }

  val xgboostParams = Map(
    "objective" -> "binary:logistic",
    "num_classes" -> 2,
    "missing" -> 0.0f,
  )

  // These params are not needed for making predictions, so we don't serialize them
  override val unserializedParams = Set("labelCol", "evalMetric")

  // The pure-JVM predictor path only produces the probability column (rawPrediction,
  // leaf_prediction, and contrib_prediction are dropped for performance / are non-goals), so those
  // Spark-only columns are excluded from the parity comparison.
  override val excludedColsForComparison =
    Array[String]("prediction", "rawPrediction", "leaf_prediction", "contrib_prediction")

  // The predictor computes the sigmoid in float32 to match native XGBoost, so probabilities agree
  // with Spark's Booster to ~1 float32 ULP (~1.2e-7 absolute). The default relTol 1e-6 is tighter
  // than float32 can hold, so relax it slightly; this is rounding, not a semantic difference.
  relTolEps = 1e-5

  val sparkTransformer: Transformer = {
    val featureAssembler = new VectorAssembler()
      .setInputCols(Array("AT", "V", "AP", "RH"))
      .setOutputCol("features")
    val classifier = createClassifier(xgboostParams, featureAssembler, dataset, "PE")
    SparkUtil.createPipelineModel(Array(featureAssembler, classifier))
  }

  private def createClassifier(xgboostParams: Map[String, Any],
                       featurePipeline: Transformer,
                       dataset: DataFrame,
                       labelCol: String): Transformer ={
    new XGBoostClassifier(xgboostParams).
      setFeaturesCol("features").
      setProbabilityCol("probabilities").
      setLabelCol(labelCol).
      fit(featurePipeline.transform(dataset)).
      setLeafPredictionCol("leaf_prediction").
      setContribPredictionCol("contrib_prediction")
  }
}
