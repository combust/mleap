package ml.combust.mleap.xgboost.runtime.testing

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.xgboost.runtime.{XGBoostRegression, XGBoostRegressionModel}

object RegressionUtils extends BoosterUtils with CachedDatasetUtils with BundleSerializationUtils {
  val xgboostBooster = trainBooster(binomialDataset)
  val mleapTransformer: Transformer = XGBoostRegression(
      "xgboostSingleThread",
      NodeShape.regression(),
      XGBoostRegressionModel(xgboostBooster, numFeatures(leapFrameBinomial), 0)
    )
  val mleapBundle = serializeModelToMleapBundle(mleapTransformer)
  val deserializedXGBoostPredictor = loadXGBoostPredictorFromBundle(mleapBundle)
  val deserializedMleapTransformer = loadMleapTransformerFromBundle(mleapBundle)
}
