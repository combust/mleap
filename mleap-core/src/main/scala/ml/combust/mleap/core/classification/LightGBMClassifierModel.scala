package ml.combust.mleap.core.classification

import com.microsoft.ml.spark.lightgbm.LightGBMBooster
import org.apache.spark.ml.linalg.{Vector, Vectors}

object LightGBMClassifierModel{
  def apply(model: String,
            labelColName: String,
            featuresColName: String,
            predictionColName: String,
            probColName: String,
            rawPredictionColName: String,
            actualNumClasses: Int): LightGBMClassifierModel = LightGBMClassifierModel(
    model, labelColName, featuresColName, predictionColName, probColName, rawPredictionColName,
    actualNumClasses = actualNumClasses)
}

case class LightGBMClassifierModel(
                         override val booster: LightGBMBooster,
                         override val labelColName: String,
                         override val featuresColName: String,
                         override val predictionColName: String,
                         override val probColName: String,
                         override val rawPredictionColName: String,
                         override val thresholdValues: Option[Seq[Double]],
                         override val actualNumClasses: Int)
  extends ProbabilisticClassificationModel with LightGBMClassifierModelBase with Serializable {
  override val numClasses: Int = actualNumClasses

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    throw new NotImplementedError("Unexpected error in LightGBMClassificationModel:" +
      " raw2probabilityInPlace should not be called!")
  }

  override def predictRaw(features: Vector): Vector = {
    Vectors.dense(booster.score(features, true, true))
  }

  override def predictProbabilities(features: Vector): Vector = {
    Vectors.dense(booster.score(features, false, true))
  }

  override def predict(features: Vector): Double = {
    rawToPrediction(predictRaw(features))
  }

  override val numFeatures: Int = 0
}

trait LightGBMClassifierModelBase {
  def booster: LightGBMBooster
  def labelColName: String
  def featuresColName: String
  def predictionColName: String
  def probColName: String
  def rawPredictionColName: String
  def thresholdValues: Option[Seq[Double]]
  def actualNumClasses: Int
}
