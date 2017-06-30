package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.RegexTokenizerModel
import ml.combust.mleap.core.types.{ListType, StringType, StructField}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

import scala.util.{Success, Try}

case class RegexTokenizer(override val uid: String = Transformer.uniqueName("regex_tokenizer"),
                          override val inputCol: String,
                          override val outputCol: String,
                          model: RegexTokenizerModel
                         ) extends FeatureTransformer {

  override val exec: UserDefinedFunction = (value: String) => model(value)

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, StringType()),
    StructField(outputCol, ListType(StringType()))
  ))
}
