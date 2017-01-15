package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.NormalizerModel
import ml.combust.mleap.core.tensor.Tensor
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

/**
  * Created by hollinwilkins on 9/24/16.
  */
case class Normalizer(override val uid: String = Transformer.uniqueName("normalizer"),
                      override val inputCol: String,
                      override val outputCol: String,
                      model: NormalizerModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Tensor[Double]) => model(value): Tensor[Double]
}
