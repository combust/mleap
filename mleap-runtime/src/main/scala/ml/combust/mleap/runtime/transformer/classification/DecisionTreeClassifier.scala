package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.DecisionTreeClassifierModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.spark.ml.linalg.Vector

import scala.util.Try

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class DecisionTreeClassifier(override val uid: String = Transformer.uniqueName("decision_tree_classification"),
                                  featuresCol: String,
                                  predictionCol: String,
                                  rawPredictionCol: Option[String] = None,
                                  probabilityCol: Option[String] = None,
                                  model: DecisionTreeClassifierModel) extends Transformer {
  val predictRaw: UserDefinedFunction = (features: Vector) => model.predictRaw(features)
  val rawToProbability: UserDefinedFunction = (raw: Vector) => model.rawToProbability(raw)
  val rawToPrediction: UserDefinedFunction = (raw: Vector) => model.rawToPrediction(raw)
  val probabilityToPrediction: UserDefinedFunction = (raw: Vector) => model.probabilityToPrediction(raw)
  val predictProbabilities: UserDefinedFunction = (features: Vector) => model.predictProbabilities(features)
  val predict: UserDefinedFunction = (features: Vector) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    (rawPredictionCol, probabilityCol) match {
      case ((Some(rp), Some(p))) =>
        for(b <- builder.withOutput(rp, featuresCol)(predictRaw);
            b2 <- b.withOutput(p, rp)(rawToProbability);
            b3 <- b2.withOutput(predictionCol, p)(probabilityToPrediction)) yield b3
      case ((Some(rp), None)) =>
        for(b <- builder.withOutput(rp, featuresCol)(predictRaw);
            b2 <- b.withOutput(predictionCol, rp)(rawToPrediction)) yield b2
      case (None, Some(p)) =>
        for(b <- builder.withOutput(p, featuresCol)(predictProbabilities);
            b2 <- b.withOutput(predictionCol, p)(probabilityToPrediction)) yield b2
      case (None, None) =>
        builder.withOutput(predictionCol, featuresCol)(predict)
    }
  }
}
