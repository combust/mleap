package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.OneVsRestModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class OneVsRest(override val uid: String = Transformer.uniqueName("one_vs_rest"),
                     featuresCol: String,
                     predictionCol: String,
                     probabilityCol: Option[String] = None,
                     model: OneVsRestModel) extends Transformer {
  val predictProbability: UserDefinedFunction = (features: Tensor[Double]) => model.predictProbability(features)
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    probabilityCol match {
      case Some(p) =>
        for(b <- builder.withOutput(p, featuresCol)(predictProbability);
            b2 <- b.withOutput(predictionCol, featuresCol)(exec)) yield b2
      case None =>
        builder.withOutput(predictionCol, featuresCol)(exec)
    }
  }
}
