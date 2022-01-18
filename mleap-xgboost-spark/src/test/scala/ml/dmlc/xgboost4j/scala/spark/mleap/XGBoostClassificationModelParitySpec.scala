package ml.dmlc.xgboost4j.scala.spark.mleap

import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.mleap.SparkUtil
import org.apache.spark.ml.parity.{SparkEnv, SparkParityBase}
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class PowerPlantTableForClassifier(AT: Double, V : Double, AP : Double, RH : Double, PE : Int)

class XGBoostClassificationModelParitySpec extends SparkParityBase {

  val dataset: DataFrame = {
    import spark.sqlContext.implicits._

    spark.sqlContext.sparkContext.textFile(this.getClass.getClassLoader.getResource("datasources/xgboost_training.csv").toString)
      .map(x => x.split(","))
      .map(line => PowerPlantTableForClassifier(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble.toInt % 2))
      .toDF
  }

  val xgboostParams = Map(
    "eta" -> 0.3,
    "maxDepth" -> 2,
    "objective" -> "binary:logistic",
    "treeMethod" -> "approx",
    "earlyStoppingRounds" -> 2,
    "numRound" -> 15,
    "numClasses" -> 2,
    "nWorkers" -> 2,
    "missing" -> 0.0f,
    "allowNonZeroForMissing" -> true,
    "killSparkContextOnWorkerFailure" -> false,
  )

  // These params are not needed for making predictions, so we don't serialize them
  override val unserializedParams = Set("labelCol", "evalMetric", "objective")

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
    if (org.apache.spark.ml.parity.SparkEnv.spark.sparkContext.isStopped) {
      throw new RuntimeException("classifier DBG: spark context stopped. # 1")
    }
    try {
      new XGBoostClassifier(xgboostParams).
        setFeaturesCol("features").
        setProbabilityCol("probabilities").
        setLabelCol(labelCol).
        fit(featurePipeline.transform(dataset)).
        setLeafPredictionCol("leaf_prediction").
        setContribPredictionCol("contrib_prediction")
    } catch {
      case e: Exception =>
        if (org.apache.spark.ml.parity.SparkEnv.spark.sparkContext.isStopped) {
          throw new RuntimeException("classifier DBG: spark context stopped. # 2")
        } else {
          throw e
        }
    }
  }
}
