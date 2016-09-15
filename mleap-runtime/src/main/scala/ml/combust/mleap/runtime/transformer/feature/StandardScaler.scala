package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StandardScalerModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.TensorType

import scala.util.Try

/**
  * Created by hwilkins on 10/23/15.
  */
case class StandardScaler(uid: String = Transformer.uniqueName("standard_scaler"),
                          inputCol: String,
                          outputCol: String,
                          model: StandardScalerModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol, TensorType.doubleVector()).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, TensorType.doubleVector())(row => model(row.getVector(inputIndex)))
    }
  }
}
