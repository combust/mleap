package ml.combust.mleap.runtime.transformer.recommendation

import ml.combust.mleap.core.recommendation.ALSModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction

case class ALS(override val uid: String = Transformer.uniqueName("als"),
               override val shape: NodeShape,
               override val model: ALSModel) extends SimpleTransformer {
  val exec: UserDefinedFunction = (user: Integer, item: Integer) => model(user, item)
}
