package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.RegexIndexerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction

/**
  * Created by hwilkins on 10/22/15.
  */
case class RegexIndexer(override val uid: String = Transformer.uniqueName("string_indexer"),
                        override val shape: NodeShape,
                        override val model: RegexIndexerModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (input: String) => model(input)
}
