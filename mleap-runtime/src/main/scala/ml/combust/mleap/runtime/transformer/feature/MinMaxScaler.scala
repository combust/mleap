package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MinMaxScalerModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.TensorType

import scala.util.Try

/**
  * Created by mikhail on 9/18/16.
  */
case class MinMaxScaler(
                       uid: String = Transformer.uniqueName("min_max_scaler"),
                       inputCol: String,
                       outputCol: String,
                       model: MinMaxScalerModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol, TensorType.doubleVector()).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, TensorType.doubleVector())(row => model(row.getVector(inputIndex)))
    }
  }
}
