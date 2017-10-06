package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.{SimpleTransformer, Transformer}
import ml.combust.mleap.core.regression.IsotonicRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hollinwilkins on 12/27/16.
  */
case class IsotonicRegression(override val uid: String = Transformer.uniqueName("isotonic_regression"),
                              override val shape: NodeShape,
                              override val model: IsotonicRegressionModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = model.featureIndex match {
    case Some(_) => (features: Tensor[Double]) => model(features)
    case None => (feature: Double) => model(feature)
  }
}
