package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MultinomialLabelerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor

import scala.util.Try

/**
  * Created by hollinwilkins on 1/18/17.
  */
case class MultinomialLabeler(override val uid: String = Transformer.uniqueName("multinomial_labeler"),
                              featuresCol: String,
                              probabilitiesCol: String,
                              labelsCol: String,
                              model: MultinomialLabelerModel) extends Transformer {
  val probabilitiesExec: UserDefinedFunction = (t: Tensor[Double]) => model.top(t).map(_._1)
  val labelsExec: UserDefinedFunction = (t: Tensor[Double]) => model.topLabels(t)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    for(b <- builder.withOutput(probabilitiesCol, featuresCol)(probabilitiesExec);
        b2 <- b.withOutput(labelsCol, featuresCol)(labelsExec)) yield b2
  }
}
