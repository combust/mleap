package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.{SimpleTransformer, Transformer}
import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

case class LinearRegression(override val uid: String = Transformer.uniqueName("linear_regression"),
                            override val shape: NodeShape,
                            override val model: LinearRegressionModel) extends SimpleTransformer {
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)
}
