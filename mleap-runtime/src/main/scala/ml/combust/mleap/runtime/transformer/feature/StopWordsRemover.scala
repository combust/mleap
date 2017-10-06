package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.{SimpleTransformer, Transformer}
import ml.combust.mleap.core.feature.StopWordsRemoverModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.core.function.UserDefinedFunction

/**
  * Created by mikhail on 10/16/16.
  */
case class StopWordsRemover(override val uid:String = Transformer.uniqueName("stop_words_remover"),
                            override val shape: NodeShape,
                            override val model: StopWordsRemoverModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (value: Seq[String]) => model(value)
}
