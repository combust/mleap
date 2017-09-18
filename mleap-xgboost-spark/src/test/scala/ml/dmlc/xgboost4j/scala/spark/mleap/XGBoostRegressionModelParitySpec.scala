package ml.dmlc.xgboost4j.scala.spark.mleap

import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by hollinwilkins on 9/16/17.
  */
class XGBoostRegressionModelParitySpec extends SparkParityBase {
  private val xgboostParams: Map[String, Any] = Map(
    "eta" -> 0.1,
    "max_depth" -> 2,
    "objective" -> "reg:linear"
  )

  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti", "loan_amount")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new OneHotEncoder().
      setInputCol("fico_index").
      setOutputCol("fico"),
    new VectorAssembler().
      setInputCols(Array("fico", "dti")).
      setOutputCol("features"),
    new XGBoostEstimator(xgboostParams).
      setFeaturesCol("features").
      setLabelCol("loan_amount").
      setPredictionCol("prediction"))).fit(dataset)

  override def equalityTest(sparkDataset: Array[Row],
                            mleapDataset: Array[Row]): Boolean = {
    !sparkDataset.zip(mleapDataset).exists {
      case (sp, ml) =>
        val v1 = sp.getFloat(6)
        val v2 = ml.getDouble(6)
        Math.abs(v2 - v1) > 0.0000001
    }
  }
}
