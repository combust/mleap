package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.RegexTokenizer

class RegexTokenizerOp extends OpNode[SparkBundleContext, RegexTokenizer, RegexTokenizer] {
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
        .withAttr(RegexIdentifier, Value.string(obj.getPattern))
        .withAttr(MatchGapsIdentifier, Value.boolean(obj.getGaps))
        .withAttr(MinTokenLengthIdentifer, Value.int(obj.getMinTokenLength))
        .withAttr(LowercaseText, Value.boolean(obj.getToLowercase))
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

  override val klazz: Class[RegexTokenizer] = classOf[RegexTokenizer]

  override def name(node: RegexTokenizer): String = node.uid

  override def model(node: RegexTokenizer): RegexTokenizer = node

  override def load(node: Node, model: RegexTokenizer)
                   (implicit context: BundleContext[SparkBundleContext]): RegexTokenizer = {
    new RegexTokenizer(uid = node.name)
      .setInputCol(node.shape.standardInput.name)
      .setOutputCol(node.shape.standardOutput.name)
      .setPattern(model.getPattern)
      .setGaps(model.getGaps)
      .setMinTokenLength(model.getMinTokenLength)
      .setToLowercase(model.getToLowercase)
  }

  override def shape(node: RegexTokenizer)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    Shape().withStandardIO(node.getInputCol, node.getOutputCol)
  }
}
