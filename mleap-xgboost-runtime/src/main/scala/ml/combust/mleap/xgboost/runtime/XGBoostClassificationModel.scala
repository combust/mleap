package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.classification.ProbabilisticClassificationModel
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import ml.combust.mleap.tensor.Tensor
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import XgbConverters._

trait XGBoostClassificationModelBase extends ProbabilisticClassificationModel {
  def booster: Booster
  def treeLimit: Int

  /** The value XGBoost reads as absent, as the model was trained with (NaN unless set). */
  def missing: Float

  override def predict(features: Vector): Double = predict(features.asXGB(missing))
  def predict(data: DMatrix): Double

  override def predictRaw(features: Vector): Vector = predictRaw(features.asXGB(missing))
  def predictRaw(data: DMatrix): Vector

  override def predictProbabilities(features: Vector): Vector = predictProbabilities(features.asXGB(missing))
  def predictProbabilities(data: DMatrix): Vector

  def predictLeaf(features: Vector): Tensor[Double] = predictLeaf(features.asXGB(missing))
  def predictLeaf(data: DMatrix): Tensor[Double] = Tensor.denseVector(booster.predictLeaf(data, treeLimit = treeLimit).head.map(_.toDouble))

  def predictContrib(features: Vector): Tensor[Double] = predictContrib(features.asXGB(missing))
  def predictContrib(data: DMatrix): Tensor[Double] = Tensor.denseVector(booster.predictContrib(data, treeLimit = treeLimit).head.map(_.toDouble))
}

case class XGBoostBinaryClassificationModel(override val booster: Booster,
                                            override val numFeatures: Int,
                                            override val treeLimit: Int,
                                            override val missing: Float = Float.NaN) extends XGBoostClassificationModelBase {
  override val numClasses: Int = 2

  def predict(data: DMatrix): Double = {
    Math.round(booster.predict(data, outPutMargin = false, treeLimit = treeLimit).head(0))
  }

  def predictProbabilities(data: DMatrix): Vector = {
    val m = booster.predict(data, outPutMargin = false, treeLimit = treeLimit).head(0)
    Vectors.dense(1 - m, m)
  }

  def predictRaw(data: DMatrix): Vector = {
    val m = booster.predict(data, outPutMargin = true, treeLimit = treeLimit).head(0)
    Vectors.dense(- m, m)
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    throw new Exception("XGBoost Classification model does not support \'rawToProbabilityInPlace\'")
  }
}

case class XGBoostMultinomialClassificationModel(override val booster: Booster,
                                                 override val numClasses: Int,
                                                 override val numFeatures: Int,
                                                 override val treeLimit: Int,
                                                 override val missing: Float = Float.NaN) extends XGBoostClassificationModelBase {

  override def predict(data: DMatrix): Double = {
    probabilityToPrediction(predictProbabilities(data))
  }

  def predictProbabilities(data: DMatrix): Vector = {
    Vectors.dense(booster.predict(data, outPutMargin = false, treeLimit = treeLimit).head.map(_.toDouble))
  }

  def predictRaw(data: DMatrix): Vector = {
    Vectors.dense(booster.predict(data, outPutMargin = true, treeLimit = treeLimit).head.map(_.toDouble))
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    throw new Exception("XGBoost Classification model does not support \'rawToProbabilityInPlace\'")
  }
}

case class XGBoostClassificationModel(impl: XGBoostClassificationModelBase) extends ProbabilisticClassificationModel {
  override val numClasses: Int = impl.numClasses
  override val numFeatures: Int = impl.numFeatures
  def treeLimit: Int = impl.treeLimit
  def missing: Float = impl.missing

  def booster: Booster = impl.booster

  def binaryClassificationModel: XGBoostBinaryClassificationModel = impl.asInstanceOf[XGBoostBinaryClassificationModel]
  def multinomialClassificationModel: XGBoostMultinomialClassificationModel = impl.asInstanceOf[XGBoostMultinomialClassificationModel]

  def predict(data: DMatrix): Double = impl.predict(data)

  def predictLeaf(features: Vector): Tensor[Double] = impl.predictLeaf(features)
  def predictLeaf(data: DMatrix): Tensor[Double] = impl.predictLeaf(data)

  def predictContrib(features: Vector): Tensor[Double] = impl.predictContrib(features)
  def predictContrib(data: DMatrix): Tensor[Double] = impl.predictContrib(data)

  override def predictProbabilities(features: Vector): Vector = impl.predictProbabilities(features)
  def predictProbabilities(data: DMatrix): Vector = impl.predictProbabilities(data)

  override def predictRaw(features: Vector): Vector = impl.predictRaw(features)
  def predictRaw(data: DMatrix): Vector = impl.predictRaw(data)

  override def rawToProbabilityInPlace(raw: Vector): Vector = impl.rawToProbabilityInPlace(raw)

  override def outputSchema: StructType = StructType("raw_prediction" -> TensorType.Double(numClasses),
    "probability" -> TensorType.Double(numClasses),
    "prediction" -> ScalarType.Double.nonNullable,
    "leaf_prediction" -> TensorType.Double(),
    "contrib_prediction" -> TensorType.Double()).get
}
