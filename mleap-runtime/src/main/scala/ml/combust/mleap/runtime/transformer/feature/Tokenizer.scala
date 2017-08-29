package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.TokenizerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}

/**
  * Created by hwilkins on 12/30/15.
  */
case class Tokenizer(override val uid: String = Transformer.uniqueName("tokenizer"),
                     override val shape: NodeShape,
                     override val model: TokenizerModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (value: String) => model(value)
}
