package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.TokenizerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.runtime.types.{ListType, StringType, StructField}

import scala.util.{Success, Try}

/**
  * Created by hwilkins on 12/30/15.
  */
case class Tokenizer(override val uid: String = Transformer.uniqueName("tokenizer"),
                     override val inputCol: String,
                     override val outputCol: String) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: String) => TokenizerModel.defaultTokenizer(value)

  override def getSchema(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, StringType()),
    StructField(outputCol, ListType(StringType()))
  ))
}
