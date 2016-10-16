package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StopWordsRemoverModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{StringType, ListType}

import scala.util.Try

/**
  * Created by mikhail on 10/16/16.
  */
case class StopWordsRemover(uid:String = Transformer.uniqueName("stopwords_remover"),
                            inputCol: String,
                            outputCol: String,
                            model: StopWordsRemoverModel) extends Transformer{
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol, ListType(StringType)).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, ListType(StringType))(row => model(row.getArray(inputIndex)))
    }
  }
}
