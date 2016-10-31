package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.RandomForestClassifierModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.spark.ml.linalg.Vector

import scala.util.Try

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class RandomForestClassifier(override val uid: String = Transformer.uniqueName("random_forest_classification"),
                                  featuresCol: String,
                                  predictionCol: String,
                                  rawPredictionCol: Option[String] = None,
                                  probabilityCol: Option[String] = None,
                                  model: RandomForestClassifierModel) extends Transformer {
  val predictRaw: UserDefinedFunction = (features: Vector) => model.predictRaw(features)
  val rawToProbability: UserDefinedFunction = (raw: Vector) => model.rawToProbability(raw)
  val rawToPrediction: UserDefinedFunction = (raw: Vector) => model.rawToPrediction(raw)
  val probabilityToPrediction: UserDefinedFunction = (raw: Vector) => model.probabilityToPrediction(raw)
  val predict: UserDefinedFunction = (features: Vector) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    rawPredictionCol match {
      case Some(rp) =>
        probabilityCol match {
          case Some(p) =>
            for(b <- builder.withOutput(rp, featuresCol)(predictRaw);
                b2 <- b.withOutput(p, rp)(rawToProbability);
                b3 <- b2.withOutput(predictionCol, p)(probabilityToPrediction)) yield b3
          case None =>
            for(b <- builder.withOutput(rp, featuresCol)(predictRaw);
                b2 <- b.withOutput(predictionCol, rp)(rawToPrediction)) yield b2
        }
      case None =>
        builder.withOutput(predictionCol, featuresCol)(predict)
    }
  }
}
