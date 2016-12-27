package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MathUnaryModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

/**
  * Created by hollinwilkins on 12/27/16.
  */
case class MathUnary(override val uid: String = Transformer.uniqueName("math_unary"),
                     override val inputCol: String,
                     override val outputCol: String,
                     model: MathUnaryModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (a: Double) => model(a)
}
