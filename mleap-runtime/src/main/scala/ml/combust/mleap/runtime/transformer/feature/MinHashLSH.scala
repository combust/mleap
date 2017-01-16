package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MinHashLSHModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.FeatureTransformer
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.runtime.converter.VectorConverters._

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class MinHashLSH(override val uid: String,
                      override val inputCol: String,
                      override val outputCol: String,
                      model: MinHashLSHModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features).map(v => v: Tensor[Double])
}
