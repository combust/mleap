package ml.combust.mleap.xgboost

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class XGBoostRegression(override val uid: String = Transformer.uniqueName("xgboost.regression"),
                             override val shape: NodeShape,
                             override val model: XGBoostRegressionModel) extends SimpleTransformer {
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model.predictDouble(features)
}
