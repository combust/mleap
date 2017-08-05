package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.RegexTokenizer

class RegexTokenizerOp extends SimpleSparkOp[RegexTokenizer] {
  override val Model: OpModel[SparkBundleContext, RegexTokenizer] = new OpModel[SparkBundleContext, RegexTokenizer] {

    val RegexIdentifier = "regex"
    val MatchGapsIdentifier = "match_gaps"
    val MinTokenLengthIdentifer = "token_min_length"
    val LowercaseText = "lowercase_text"

    override val klazz: Class[RegexTokenizer] = classOf[RegexTokenizer]

    override def opName: String = Bundle.BuiltinOps.feature.regex_tokenizer

    override def store(model: Model, obj: RegexTokenizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {

      model
        .withValue(RegexIdentifier, Value.string(obj.getPattern))
        .withValue(MatchGapsIdentifier, Value.boolean(obj.getGaps))
        .withValue(MinTokenLengthIdentifer, Value.int(obj.getMinTokenLength))
        .withValue(LowercaseText, Value.boolean(obj.getToLowercase))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): RegexTokenizer = {

      new RegexTokenizer(uid = "")
        .setPattern(model.value(RegexIdentifier).getString)
        .setGaps(model.value(MatchGapsIdentifier).getBoolean)
        .setMinTokenLength(model.value(MinTokenLengthIdentifer).getInt)
        .setToLowercase(model.value(LowercaseText).getBoolean)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: RegexTokenizer): RegexTokenizer = {
    new RegexTokenizer(uid = uid)
    .setPattern(model.getPattern)
    .setGaps(model.getGaps)
    .setMinTokenLength(model.getMinTokenLength)
    .setToLowercase(model.getToLowercase)
  }

  override def sparkInputs(obj: RegexTokenizer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: RegexTokenizer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
