package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.DCTModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class DCT(override val uid: String = Transformer.uniqueName("dct"),
               override val shape: NodeShape,
               override val model: DCTModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features): Tensor[Double]
}
