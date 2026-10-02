package ml.combust.mleap.xgboost.runtime

import com.yelp.xgboost.Predictor
import com.yelp.xgboost.FVec
import ml.combust.mleap.core.classification.ProbabilisticClassificationModel
import org.apache.spark.ml.linalg.{Vector, Vectors}
import ml.combust.mleap.core.types.{StructType, TensorType}
import XgbConverters._


trait XGBoostPredictorClassificationModelBase extends ProbabilisticClassificationModel {
  def predictor: Predictor
  def treeLimit: Int

  /** True when the model was trained with missing=0.0f, so 0.0 features are treated as absent. */
  def treatsZeroAsNA: Boolean

  override def predict(features: Vector): Double = predict(features.asXGBPredictor(treatsZeroAsNA))
  def predict(data: FVec): Double

  override def predictRaw(features: Vector): Vector = predictRaw(features.asXGBPredictor(treatsZeroAsNA))
  def predictRaw(data: FVec): Vector

  override def predictProbabilities(features: Vector): Vector = predictProbabilities(features.asXGBPredictor(treatsZeroAsNA))
  def predictProbabilities(data: FVec): Vector

  def predictLeaf(features: Vector): Seq[Double] = predictLeaf(features.asXGBPredictor(treatsZeroAsNA))
  def predictLeaf(data: FVec): Seq[Double] = predictor.predictLeaf(data, treeLimit).map(_.toDouble)
}

case class XGBoostPredictorBinaryClassificationModel(
      override val predictor: Predictor,
      override val numFeatures: Int,
      override val treeLimit: Int,
      override val treatsZeroAsNA: Boolean = false) extends XGBoostPredictorClassificationModelBase {

  override val numClasses: Int = 2

  def predict(data: FVec): Double =
    Math.round(predictor.predict(data, treeLimit).head)

  def predictProbabilities(data: FVec): Vector = {
    val m = predictor.predict(data, treeLimit).head
    Vectors.dense(1 - m, m)
  }

  def predictRaw(data: FVec): Vector = {
    val m = predictor.predictRaw(data, treeLimit).head
    Vectors.dense(- m, m)
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    throw new Exception("XGBoost Classification model does not support \'rawToProbabilityInPlace\'")
  }
}

case class XGBoostPredictorMultinomialClassificationModel(
                       override val predictor: Predictor,
                       override val numClasses: Int,
                       override val numFeatures: Int,
                       override val treeLimit: Int,
                       override val treatsZeroAsNA: Boolean = false) extends XGBoostPredictorClassificationModelBase {

  override def predict(data: FVec): Double = {
    probabilityToPrediction(predictProbabilities(data))
  }

  def predictProbabilities(data: FVec): Vector = {
    Vectors.dense(predictor.predict(data, treeLimit).map(_.toDouble))
  }

  def predictRaw(data: FVec): Vector = {
    Vectors.dense(predictor.predictRaw(data, treeLimit).map(_.toDouble))
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    throw new Exception("XGBoost Classification model does not support \'rawToProbabilityInPlace\'")
  }
}

case class XGBoostPredictorClassificationModel(impl: XGBoostPredictorClassificationModelBase) extends ProbabilisticClassificationModel {
  override val numClasses: Int = impl.numClasses
  override val numFeatures: Int = impl.numFeatures
  def treeLimit: Int = impl.treeLimit
  def treatsZeroAsNA: Boolean = impl.treatsZeroAsNA

  def predictor: Predictor = impl.predictor

  def binaryClassificationModel: XGBoostPredictorBinaryClassificationModel = impl.asInstanceOf[XGBoostPredictorBinaryClassificationModel]
  def multinomialClassificationModel: XGBoostPredictorMultinomialClassificationModel = impl.asInstanceOf[XGBoostPredictorMultinomialClassificationModel]

  def predict(data: FVec): Double = impl.predict(data)

  def predictLeaf(features: Vector): Seq[Double] = impl.predictLeaf(features)
  def predictLeaf(data: FVec): Seq[Double] = impl.predictLeaf(data)

  override def predictProbabilities(features: Vector): Vector = impl.predictProbabilities(features)
  def predictProbabilities(data: FVec): Vector = impl.predictProbabilities(data)

  override def predictRaw(features: Vector): Vector = impl.predictRaw(features)
  def predictRaw(data: FVec): Vector = impl.predictRaw(data)

  override def rawToProbabilityInPlace(raw: Vector): Vector = impl.rawToProbabilityInPlace(raw)

  override def outputSchema: StructType = StructType(
    "probability" -> TensorType.Double(numClasses)
  ).get
}
