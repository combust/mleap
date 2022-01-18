package ml.dmlc.xgboost4j.scala.spark.mleap

import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.mleap.SparkUtil
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class PowerPlantTable(AT: Double, V : Double, AP : Double, RH : Double, PE : Double)

class XGBoostRegressionModelParitySpec extends SparkParityBase {

  private val xgboostParams: Map[String, Any] = Map(
    "eta" -> 0.3,
    "maxDepth" -> 2,
    "missing" -> 0.0f,
    "objective" -> "reg:squaredlogerror",
    "treeMethod" -> "approx",
    "earlyStoppingRounds" -> 2,
    "numRound" -> 15,
    "allowNonZeroForMissing" -> true,
    "killSparkContextOnWorkerFailure" -> false,
  )

  // These params are not needed for making predictions, so we don't serialize them
  override val unserializedParams = Set("labelCol", "evalMetric", "objective")

  val dataset: DataFrame = {
    import spark.sqlContext.implicits._

    spark.sqlContext.sparkContext.textFile(this.getClass.getClassLoader.getResource("datasources/xgboost_training.csv").toString)
      .map(x => x.split(","))
      .map(line => PowerPlantTable(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble))
      .toDF
  }

  val sparkTransformer: Transformer = {
    val featureAssembler = new VectorAssembler()
      .setInputCols(Array("AT", "V", "AP", "RH"))
      .setOutputCol("features")
    if (org.apache.spark.ml.parity.SparkEnv.spark.sparkContext.isStopped) {
      throw new RuntimeException("regressor DBG: spark context stopped. # 1")
    }
    try {
      val regressor = new XGBoostRegressor(xgboostParams).
        setFeaturesCol("features").
        setLabelCol("PE").
        setPredictionCol("prediction").
        fit(featureAssembler.transform(dataset)).
        setLeafPredictionCol("leaf_prediction").
        setContribPredictionCol("contrib_prediction")
    } catch {
    case e: _ =>
      if (org.apache.spark.ml.parity.SparkEnv.spark.sparkContext.isStopped) {
        throw new RuntimeException("regressor DBG: spark context stopped. # 2")
      } else {
        throw e
      }
  }
    SparkUtil.createPipelineModel(Array(featureAssembler, regressor))
  }
}
