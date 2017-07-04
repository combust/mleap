package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ChiSqSelectorModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hollinwilkins on 12/27/16.
  */
case class ChiSqSelector(override val uid: String = Transformer.uniqueName("chi_sq_selector"),
                         override val shape: NodeShape,
                         model: ChiSqSelectorModel) extends SimpleTransformer {
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features): Tensor[Double]
}
