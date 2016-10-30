package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.LogisticRegressionModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.spark.ml.linalg.Vector

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class LogisticRegression(override val uid: String = Transformer.uniqueName("logistic_regression"),
                              featuresCol: String,
                              predictionCol: String,
                              probabilityCol: Option[String] = None,
                              model: LogisticRegressionModel) extends Transformer {
  val predictProbability: UserDefinedFunction = (features: Vector) => model.predictProbability(features)
  val probabilityToPrediction: UserDefinedFunction = (probability: Double) => model.probabilityToPrediction(probability)
  val exec: UserDefinedFunction = (features: Vector) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    probabilityCol match {
      case Some(p) =>
        for(b <- builder.withOutput(p, featuresCol)(predictProbability);
            b2 <- b.withOutput(predictionCol, p)(probabilityToPrediction)) yield b2
      case None =>
        builder.withOutput(predictionCol, featuresCol)(exec)
    }
  }
}
