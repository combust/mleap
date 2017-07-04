package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MathBinaryModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}

/**
  * Created by hollinwilkins on 12/27/16.
  */
case class MathBinary(override val uid: String = Transformer.uniqueName("math_binary"),
                      override val shape: NodeShape,
                      model: MathBinaryModel) extends SimpleTransformer {
  val execAB: UserDefinedFunction = (a: Double, b: Double) => model(Some(a), Some(b))
  val execA: UserDefinedFunction = (a: Double) => model(Some(a), None)
  val execB: UserDefinedFunction = (b: Double) => model(None, Some(b))
  val execNone: UserDefinedFunction = () => model(None, None)

  override val exec: UserDefinedFunction = {
    (shape.getInput("input_a"), shape.getInput("input_b")) match {
      case (Some(_), Some(_)) => execAB
      case (Some(_), None) => execA
      case (None, Some(_)) => execB
      case (None, None) => execNone
    }
  }
}
