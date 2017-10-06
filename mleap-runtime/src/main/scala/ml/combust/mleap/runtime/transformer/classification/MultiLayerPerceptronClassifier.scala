package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.{SimpleTransformer, Transformer}
import ml.combust.mleap.core.classification.MultiLayerPerceptronClassifierModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hollinwilkins on 12/25/16.
  */
case class MultiLayerPerceptronClassifier(override val uid: String = Transformer.uniqueName("multi_layer_perceptron"),
                                          override val shape: NodeShape,
                                          override val model: MultiLayerPerceptronClassifierModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)
}
