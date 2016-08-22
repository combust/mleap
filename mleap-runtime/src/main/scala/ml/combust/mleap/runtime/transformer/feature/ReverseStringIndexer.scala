package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ReverseStringIndexerModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.StringType

import scala.util.Try

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class ReverseStringIndexer(uid: String = Transformer.uniqueName("reverse_string_indexer"),
                                inputCol: String,
                                outputCol: String,
                                model: ReverseStringIndexerModel) extends Transformer {
  override def build[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, StringType)(row => model(row.getDouble(inputIndex).toInt))
    }
  }
}
