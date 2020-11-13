package ml.combust.mleap.xgboost.runtime.testing

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.xgboost.runtime.{XGBoostRegression, XGBoostRegressionModel}
import ml.dmlc.xgboost4j.scala.Booster

trait RegressionUtils extends BoosterUtils with CachedDatasetUtils {
  def trainRegressor: Transformer = {

    val booster: Booster = trainBooster(binomialDataset)

    XGBoostRegression(
      "xgboostSingleThread",
      NodeShape.regression(),
      XGBoostRegressionModel(booster, numFeatures(leapFrameBinomial), 0)
    )
  }
}
