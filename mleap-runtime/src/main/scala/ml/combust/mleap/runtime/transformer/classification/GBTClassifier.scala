package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.GBTClassifierModel
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import ml.combust.mleap.tensor.Tensor

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 9/24/16.
  */
case class GBTClassifier(override val uid: String = Transformer.uniqueName("gbt_classifier"),
                         featuresCol: String,
                         predictionCol: String,
                         rawPredictionCol: Option[String] = None,
                         probabilityCol: Option[String] = None,
                         model: GBTClassifierModel) extends Transformer {
  val predictRaw: UserDefinedFunction = (features: Tensor[Double]) => model.predictRaw(features): Tensor[Double]
  val rawToProbability: UserDefinedFunction = (raw: Tensor[Double]) => model.rawToProbability(raw): Tensor[Double]
  val rawToPrediction: UserDefinedFunction = (raw: Tensor[Double]) => model.rawToPrediction(raw)
  val probabilityToPrediction: UserDefinedFunction = (probability: Tensor[Double]) => model.probabilityToPrediction(probability)
  val predictProbabilities: UserDefinedFunction = (features: Tensor[Double]) => model.predictProbabilities(features): Tensor[Double]
  val predict: UserDefinedFunction = (features: Tensor[Double]) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    (rawPredictionCol, probabilityCol) match {
      case (Some(rp), Some(p)) =>
        for (b <- builder.withOutput(rp, featuresCol)(predictRaw);
             b2 <- b.withOutput(p, rp)(rawToProbability);
             b3 <- b2.withOutput(predictionCol, p)(probabilityToPrediction)) yield b3
      case ((Some(rp), None)) =>
        for (b <- builder.withOutput(rp, featuresCol)(predictRaw);
             b2 <- b.withOutput(predictionCol, rp)(rawToPrediction)) yield b2
      case (None, Some(p)) =>
        for (b <- builder.withOutput(p, featuresCol)(predictProbabilities);
             b2 <- b.withOutput(predictionCol, p)(probabilityToPrediction)) yield b2
      case (None, None) =>
        builder.withOutput(predictionCol, featuresCol)(predict)
    }
  }

  override def getFields(): Try[Seq[StructField]] = {
    (rawPredictionCol, probabilityCol) match {
      case ((Some(rp), Some(p))) => Success(Seq(
        StructField(featuresCol, TensorType(DoubleType())),
        StructField(rp, TensorType(DoubleType())),
        StructField(p, TensorType(DoubleType())),
        StructField(predictionCol, DoubleType())))
      case ((Some(rp), None)) => Success(Seq(
        StructField(featuresCol, TensorType(DoubleType())),
        StructField(rp, TensorType(DoubleType())),
        StructField(predictionCol, DoubleType())))
      case (None, Some(p)) => Success(Seq(
        StructField(featuresCol, TensorType(DoubleType())),
        StructField(p, TensorType(DoubleType())),
        StructField(predictionCol, DoubleType())))
      case (None, None) => Success(Seq(
        StructField(featuresCol, TensorType(DoubleType())),
        StructField(predictionCol, DoubleType())))
    }
  }
}
