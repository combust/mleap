package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MathUnaryModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}

/**
  * Created by hollinwilkins on 12/27/16.
  */
case class MathUnary(override val uid: String = Transformer.uniqueName("math_unary"),
                     override val shape: NodeShape,
                     model: MathUnaryModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (a: Double) => model(a)
}
