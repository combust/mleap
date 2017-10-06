package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.{SimpleTransformer, Transformer}
import ml.combust.mleap.core.feature.StringMapModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.core.function.UserDefinedFunction

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class StringMap(override val uid: String = Transformer.uniqueName("string_map"),
                     override val shape: NodeShape,
                     override val model: StringMapModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (label: String) => model(label)
}
