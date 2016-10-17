package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.PolynomialExpansionModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.TensorType

import scala.util.Try

/**
  * Created by mikhail on 10/16/16.
  */
case class PolynomialExpansion(uid: String = Transformer.uniqueName("polynomial_expansion"),
                               inputCol: String,
                               outputCol: String,
                               model: PolynomialExpansionModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol, TensorType.doubleVector()).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, TensorType.doubleVector())(row => model(row.getVector(inputIndex)))
    }
  }
}
