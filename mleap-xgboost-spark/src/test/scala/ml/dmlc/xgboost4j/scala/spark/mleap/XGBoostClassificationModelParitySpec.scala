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
    import spark.sqlContext.implicits._

    spark.sqlContext.sparkContext.textFile(this.getClass.getClassLoader.getResource("datasources/xgboost_training.csv").toString)
      .map(x => x.split(","))
      .map(line => PowerPlantTableForClassifier(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble.toInt % 2))
      .toDF
  }

  val xgboostParams = Map(
    "objective" -> "binary:logistic",
    "num_classes" -> 2,
    "missing" -> 0.0f,
    "allow_non_zero_for_missing" -> true,
  )

  // These params are not needed for making predictions, so we don't serialize them
  override val unserializedParams = Set("labelCol", "evalMetric")

  override val excludedColsForComparison = Array[String]("prediction")

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
