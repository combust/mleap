package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.NGramModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}

/**
  * Created by mikhail on 10/16/16.
  */
case class NGram(override val uid: String = Transformer.uniqueName("ngram"),
                 override val shape: NodeShape,
                 override val model: NGramModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (value: Seq[String]) => model(value)
}
