package ml.combust.mleap.xgboost.runtime.testing

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.xgboost.runtime.{XGBoostBinaryClassificationModel, XGBoostClassification, XGBoostClassificationModel, XGBoostMultinomialClassificationModel}


object ClassifierUtils extends BoosterUtils with CachedDatasetUtils with BundleSerializationUtils {

  val xgboost4jBooster = trainBooster(binomialDataset)
  val mleapTransformer: Transformer = XGBoostClassification(
      "xgboostSingleThread",
      NodeShape.probabilisticClassifier(
        rawPredictionCol = Some("raw_prediction"),
        probabilityCol = Some("probability")),
      XGBoostClassificationModel(
        XGBoostBinaryClassificationModel(xgboost4jBooster, numFeatures(leapFrameBinomial), 0))
    )
  val mleapBundle = serializeModelToMleapBundle(mleapTransformer)
  val deserializedXGBoostPredictor = loadXGBoostPredictorFromBundle(mleapBundle)
  val deserializedmleapTransformer = loadMleapTransformerFromBundle(mleapBundle)

  val multinomialBooster = trainMultinomialBooster(multinomialDataset)
  val multinomialMleapTransformer: Transformer = XGBoostClassification(
      "xgboostSingleThread",
      NodeShape.probabilisticClassifier(
        rawPredictionCol = Some("raw_prediction"),
        probabilityCol = Some("probability")),
      XGBoostClassificationModel(
        XGBoostMultinomialClassificationModel(
          multinomialBooster, xgboostMultinomialParams("num_class").asInstanceOf[Int], numFeatures(leapFrameMultinomial), 0))
    )
  val multinomialMleapBundle = serializeModelToMleapBundle(multinomialMleapTransformer)
  val deserializedMultinomialXGBoostPredictor = loadXGBoostPredictorFromBundle(multinomialMleapBundle)
  val deserializedMultinomialMleapTransformer = loadMleapTransformerFromBundle(multinomialMleapBundle)
}
