package ml.combust.mleap.runtime.transformer.sklearn

import ml.combust.mleap.core.sklearn.PolynomialFeaturesModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

case class PolynomialFeatures(override val uid: String = Transformer.uniqueName("sklearn_polynomial_expansion"),
                              override val shape: NodeShape,
                              model: PolynomialFeaturesModel) extends SimpleTransformer {

  override val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features): Tensor[Double]
}