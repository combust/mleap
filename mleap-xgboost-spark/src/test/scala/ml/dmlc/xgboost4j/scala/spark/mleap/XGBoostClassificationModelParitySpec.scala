package ml.dmlc.xgboost4j.scala.spark.mleap

import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by hollinwilkins on 9/16/17.
  */
class XGBoostClassificationModelParitySpec extends SparkParityBase {
  private val xgboostParams: Map[String, Any] = Map(
    "eta" -> 0.1,
    "max_depth" -> 2,
    "objective" -> "binary:logistic"
  )

  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti", "approved")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new VectorAssembler().
      setInputCols(Array("fico_index", "dti")).
      setOutputCol("features"),
    new StringIndexer().
      setInputCol("approved").
      setOutputCol("label"),
    new XGBoostEstimator(xgboostParams).
      setFeaturesCol("features").
      setLabelCol("label"))).fit(dataset)

  override def equalityTest(sparkDataset: DataFrame,
                            mleapDataset: DataFrame): Unit = {
    val sparkProbabilityCol = sparkDataset.schema.fieldIndex("probabilities")
    val mleapProbabilityCol = mleapDataset.schema.fieldIndex("probabilities")
    val sparkPredictionCol = sparkDataset.schema.fieldIndex("prediction")
    val mleapPredictionCol = mleapDataset.schema.fieldIndex("prediction")

    assert(sparkDataset.schema.fields.length == mleapDataset.schema.fields.length)

    sparkDataset.collect().zip(mleapDataset.collect()).foreach {
      case (sp, ml) =>
        val sparkProbabilities = sp.getAs[Vector](sparkProbabilityCol).toArray
        val mleapProbabilities = ml.getAs[Vector](mleapProbabilityCol).toArray

        sparkProbabilities.zip(mleapProbabilities).foreach {
          case (v1, v2) =>
            assert(Math.abs(v2 - v1) < 0.0000001)
        }

        val sparkPrediction = sp.getDouble(sparkPredictionCol)
        val mleapPrediction = ml.getDouble(mleapPredictionCol)

        assert(sparkPrediction == mleapPrediction)
    }
  }
}
