package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.NGramModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{ListType, StringType}

import scala.util.Try

/**
  * Created by mikhail on 10/16/16.
  */
case class NGram(uid: String = Transformer.uniqueName("ngram"),
                 inputCol: String,
                 outputCol: String,
                 model: NGramModel
                ) extends Transformer{
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol, ListType(StringType)).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, ListType(StringType))(row => model(row.getArray(inputIndex)))
    }
  }
}
