package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.RegexTokenizerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

case class RegexTokenizer(override val uid: String = Transformer.uniqueName("regex_tokenizer"),
                          override val inputCol: String,
                          override val outputCol: String,
                          model: RegexTokenizerModel
                         ) extends FeatureTransformer {

  override val exec: UserDefinedFunction = (value: String) => model(value)
}
