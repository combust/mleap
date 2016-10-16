package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.PcaModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.TensorType

import scala.util.Try

/**
  * Created by hollinwilkins on 10/12/16.
  */
case class Pca(override val uid: String = Transformer.uniqueName("pca"),
               inputCol: String,
               outputCol: String,
               model: PcaModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol, TensorType.doubleVector()).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, TensorType.doubleVector())(row => model(row.getVector(inputIndex)))
    }
  }
}
