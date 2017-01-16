package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ChiSqSelectorModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.runtime.converter.VectorConverters._

import scala.util.Try

/**
  * Created by hollinwilkins on 12/27/16.
  */
case class ChiSqSelector(override val uid: String = Transformer.uniqueName("chi_sq_selector"),
                         featuresCol: String,
                         outputCol: String,
                         model: ChiSqSelectorModel) extends Transformer {
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features): Tensor[Double]

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, featuresCol)(exec)
  }
}
