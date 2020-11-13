package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import XgbConverters._


case class XGBoostPredictorRegression(override val uid: String = Transformer.uniqueName("xgboost.regression"),
                             override val shape: NodeShape,
                             override val model: XGBoostPredictorRegressionModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = {
    UserDefinedFunction(
      // Since the Predictor is our performant implementation, we only compute prediction for performance reasons.
      (features: Tensor[Double]) => model.predict(features.asXGBPredictor),
      outputSchema,
      inputSchema)
  }
}
