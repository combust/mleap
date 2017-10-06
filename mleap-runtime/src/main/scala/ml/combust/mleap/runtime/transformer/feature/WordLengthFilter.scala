package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.WordLengthFilterModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}

/**
  * Created by mageswarand on 14/2/17.
  */

case class WordLengthFilter(override val uid: String = Transformer.uniqueName("word_filter"),
                            override val shape: NodeShape,
                            override val model: WordLengthFilterModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (label: Seq[String]) => model(label)
}
