package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.PcaModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}

/**
  * Created by hollinwilkins on 10/12/16.
  */
case class Pca(override val uid: String = Transformer.uniqueName("pca"),
               override val shape: NodeShape,
               override val model: PcaModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (value: Tensor[Double]) => model(value): Tensor[Double]
}
