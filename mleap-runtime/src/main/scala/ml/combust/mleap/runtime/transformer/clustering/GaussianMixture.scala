package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.GaussianMixtureModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.spark.ml.linalg.Vector

import scala.util.Try

/**
  * Created by hollinwilkins on 11/17/16.
  */
case class GaussianMixture(override val uid: String = Transformer.uniqueName("gmm"),
                           featuresCol: String,
                           predictionCol: String,
                           probabilityCol: Option[String] = None,
                           model: GaussianMixtureModel) extends Transformer {
  val predictProbability: UserDefinedFunction = (features: Vector) => model.predictProbability(features)
  val predictionFromProbability: UserDefinedFunction = (features: Vector) => model.predictionFromProbability(features)
  val exec: UserDefinedFunction = (features: Vector) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    probabilityCol match {
      case Some(probability) =>
        for(b <- builder.withOutput(probability, featuresCol)(predictProbability);
            b2 <- b.withOutput(predictionCol, probability)(predictionFromProbability)) yield b2
      case None => builder.withOutput(predictionCol, featuresCol)(exec)
    }
  }
}
