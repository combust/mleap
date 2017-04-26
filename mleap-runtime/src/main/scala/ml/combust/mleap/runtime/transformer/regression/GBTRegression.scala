package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.GBTRegressionModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.Try

/**
  * Created by hollinwilkins on 9/24/16.
  */
case class GBTRegression(override val uid: String = Transformer.uniqueName("gbt_regression"),
                         featuresCol: String,
                         predictionCol: String,
                         model: GBTRegressionModel) extends Transformer {
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(predictionCol, featuresCol)(exec)
  }
}
