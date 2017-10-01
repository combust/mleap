package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.RegexTokenizerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}

case class RegexTokenizer(override val uid: String = Transformer.uniqueName("regex_tokenizer"),
                          override val shape: NodeShape,
                          override val model: RegexTokenizerModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (value: String) => model(value)
}