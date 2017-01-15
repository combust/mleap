package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.PcaModel
import ml.combust.mleap.core.tensor.Tensor
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

/**
  * Created by hollinwilkins on 10/12/16.
  */
case class Pca(override val uid: String = Transformer.uniqueName("pca"),
               override val inputCol: String,
               override val outputCol: String,
               model: PcaModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Tensor[Double]) => model(value): Tensor[Double]
}
