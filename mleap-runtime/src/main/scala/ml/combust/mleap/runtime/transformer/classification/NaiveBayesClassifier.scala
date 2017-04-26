package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.NaiveBayesModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.Try

/**
  * Created by fshabbir on 12/24/16.
  */
case class NaiveBayesClassifier(override val uid: String = Transformer.uniqueName("random_forest_classification"),
                                featuresCol: String,
                                predictionCol: String,
                                rawPredictionCol: Option[String] = None,
                                probabilityCol: Option[String] = None,
                                model: NaiveBayesModel) extends Transformer {
  val predictRaw: UserDefinedFunction = (features: Tensor[Double]) => model.predictRaw(features): Tensor[Double]
  val rawToProbability: UserDefinedFunction = (raw: Tensor[Double]) => model.rawToProbability(raw): Tensor[Double]
  val rawToPrediction: UserDefinedFunction = (raw: Tensor[Double]) => model.rawToPrediction(raw)
  val probabilityToPrediction: UserDefinedFunction = (raw: Tensor[Double]) => model.probabilityToPrediction(raw)
  val predictProbabilities: UserDefinedFunction = (features: Tensor[Double]) => model.predictProbabilities(features): Tensor[Double]
  val predict: UserDefinedFunction = (features: Tensor[Double]) => model(features)

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
