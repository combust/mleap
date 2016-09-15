package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.TokenizerModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{ListType, StringType}

import scala.util.Try

/**
  * Created by hwilkins on 12/30/15.
  */
case class Tokenizer(uid: String = Transformer.uniqueName("tokenizer"),
                     inputCol: String,
                     outputCol: String) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol, StringType).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, ListType(StringType))(row => TokenizerModel.defaultTokenizer(row.getString(inputIndex)))
    }
  }
}
