package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.RegexTokenizerModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.RegexTokenizer

import scala.util.matching.Regex

class RegexTokenizerOp extends MleapOp[RegexTokenizer, RegexTokenizerModel] {
  override val Model: OpModel[MleapContext, RegexTokenizerModel] = new OpModel[MleapContext, RegexTokenizerModel] {

    val RegexIdentifier = "regex"
    val MatchGapsIdentifier = "match_gaps"
    val MinTokenLengthIdentifer = "token_min_length"
    val LowercaseText = "lowercase_text"

    override val klazz: Class[RegexTokenizerModel] = classOf[RegexTokenizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.regex_tokenizer

    override def store(model: Model, obj: RegexTokenizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model
        .withValue(RegexIdentifier, Value.string(obj.regex.toString()))
        .withValue(MatchGapsIdentifier, Value.boolean(obj.matchGaps))
        .withValue(MinTokenLengthIdentifer, Value.int(obj.tokenMinLength))
        .withValue(LowercaseText, Value.boolean(obj.lowercaseText))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): RegexTokenizerModel = {
      RegexTokenizerModel(
        regex = new Regex(model.value(RegexIdentifier).getString),
        matchGaps = model.value(MatchGapsIdentifier).getBoolean,
        tokenMinLength = model.value(MinTokenLengthIdentifer).getInt,
        lowercaseText = model.value(LowercaseText).getBoolean
      )
    }
  }

  override def model(node: RegexTokenizer): RegexTokenizerModel = node.model
}
