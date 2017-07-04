package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.RandomForestRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hwilkins on 11/8/15.
  */
case class RandomForestRegression(override val uid: String = Transformer.uniqueName("random_forest_regression"),
                                  override val shape: NodeShape,
                                  model: RandomForestRegressionModel) extends SimpleTransformer {
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)
}
