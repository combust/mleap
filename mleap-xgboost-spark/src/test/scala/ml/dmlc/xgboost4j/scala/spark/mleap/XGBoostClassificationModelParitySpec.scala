package ml.dmlc.xgboost4j.scala.spark.mleap

import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.tensor.DenseTensor
import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
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

  override def equalityTest(sparkDataset: Array[Row],
                            mleapDataset: Array[Row]): Boolean = {
    !sparkDataset.zip(mleapDataset).exists {
      case (sp, ml) =>
        val sparkTensor = sp.getAs[DenseTensor[Double]](7)
        val mleapTensor = sp.getAs[DenseTensor[Double]](7)

        sparkTensor.values.zip(mleapTensor.values).exists {
          case (v1, v2) => Math.abs(v2 - v1) > 0.001
        }
    }
  }
}
