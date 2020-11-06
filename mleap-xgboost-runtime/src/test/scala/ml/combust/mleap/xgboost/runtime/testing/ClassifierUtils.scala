package ml.combust.mleap.xgboost.runtime.testing

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.xgboost.runtime.{XGBoostBinaryClassificationModel, XGBoostClassification, XGBoostClassificationModel, XGBoostMultinomialClassificationModel}
import ml.dmlc.xgboost4j.scala.Booster


trait ClassifierUtils extends BoosterUtils with CachedDatasetUtils {

  def trainXGBoost4jClassifier: Transformer = {

    val booster: Booster = trainBooster(binomialDataset)

    XGBoostClassification(
      "xgboostSingleThread",
      NodeShape.probabilisticClassifier(
        rawPredictionCol = Some("raw_prediction"),
        probabilityCol = Some("probability")),
      XGBoostClassificationModel(
        XGBoostBinaryClassificationModel(booster, numFeatures(leapFrameBinomial), 0))
    )
  }

  def trainMultinomialXGBoost4jClassifier: Transformer ={

    val booster: Booster = trainMultinomialBooster(multinomialDataset)

    XGBoostClassification(
      "xgboostSingleThread",
      NodeShape.probabilisticClassifier(
        rawPredictionCol = Some("raw_prediction"),
        probabilityCol = Some("probability")),
      XGBoostClassificationModel(
        XGBoostMultinomialClassificationModel(
          booster, xgboostMultinomialParams("num_class").asInstanceOf[Int], numFeatures(leapFrameMultinomial), 0))
    )
  }
}
