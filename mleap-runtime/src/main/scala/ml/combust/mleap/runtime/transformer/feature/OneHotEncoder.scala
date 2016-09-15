package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, TensorType}

import scala.util.Try

/**
  * Created by hollinwilkins on 5/10/16.
  */
case class OneHotEncoder(uid: String = Transformer.uniqueName("one_hot_encoder"),
                         inputCol: String,
                         outputCol: String,
                         model: OneHotEncoderModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol, DoubleType).flatMap {
      case(b, index) =>
        b.withOutput(outputCol, TensorType.doubleVector())(row => model(row.getDouble(index)))
    }
  }
}
