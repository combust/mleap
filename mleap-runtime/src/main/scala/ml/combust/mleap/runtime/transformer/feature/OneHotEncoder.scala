package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hollinwilkins on 5/10/16.
  */
case class OneHotEncoder(override val uid: String = Transformer.uniqueName("one_hot_encoder"),
                         override val shape: NodeShape,
                         override val model: OneHotEncoderModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (value: Double) => model(value): Tensor[Double]
}
