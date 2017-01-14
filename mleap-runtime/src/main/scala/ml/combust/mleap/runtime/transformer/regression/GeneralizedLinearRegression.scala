package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.Tensor
import ml.combust.mleap.core.regression.GeneralizedLinearRegressionModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class GeneralizedLinearRegression(override val uid: String = Transformer.uniqueName("generalized_lr"),
                                       featuresCol: String,
                                       predictionCol: String,
                                       linkPredictionCol: Option[String] = None,
                                       model: GeneralizedLinearRegressionModel) extends Transformer {
  val predict: UserDefinedFunction = (features: Tensor[Double]) => model.predict(features)
  val predictLink: UserDefinedFunction = (features: Tensor[Double]) => model.predictLink(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    linkPredictionCol match {
      case Some(col) =>
        for(b1 <- builder.withOutput(predictionCol, featuresCol)(predict);
            b2 <- b1.withOutput(col, featuresCol)(predictLink)) yield b2
      case None => builder.withOutput(predictionCol, featuresCol)(predict)
    }
  }
}
