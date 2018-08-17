package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.classification.ProbabilisticClassificationModel
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix}
import org.apache.spark.ml.linalg.{Vector, Vectors}

trait XGBoostClassificationModelBase extends ProbabilisticClassificationModel {
  val booster: Booster
}

case class XGBoostBinaryClassificationModel(override val booster: Booster,
                                            override val numFeatures: Int) extends XGBoostClassificationModelBase {
  override val numClasses: Int = 2

  override def predict(features: Vector): Double = {
    val data = new DMatrix(Iterator(new LabeledPoint(0.0f, null, features.toDense.values.map(_.toFloat))))
    val m = booster.predict(data).head(0)
    Math.round(m)
  }

  override def predictProbabilities(features: Vector): Vector = {
    val data = new DMatrix(Iterator(new LabeledPoint(0.0f, null, features.toDense.values.map(_.toFloat))))
    val m = booster.predict(data).head(0)
    Vectors.dense(1 - m, m)
  }

  override def predictRaw(features: Vector): Vector = {
    val data = new DMatrix(Iterator(new LabeledPoint(0.0f, null, features.toDense.values.map(_.toFloat))))
    val m = booster.predict(data, outPutMargin = true).head(0)
    Vectors.dense(1 - m, m)
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    throw new Exception("XGBoost Classification model does not support \'rawToProbabilityInPlace\'")
  }
}

case class XGBoostMultinomialClassificationModel(override val booster: Booster,
                                                 override val numClasses: Int,
                                                 override val numFeatures: Int) extends XGBoostClassificationModelBase {

  override def predict(features: Vector): Double = {
    probabilityToPrediction(predictProbabilities(features))
  }

  override def predictProbabilities(features: Vector): Vector = {
    val data = new DMatrix(Iterator(new LabeledPoint(0.0f, null, features.toDense.values.map(_.toFloat))))
    Vectors.dense(booster.predict(data).head.map(_.toDouble))
  }

  override def predictRaw(features: Vector): Vector = {
    val data = new DMatrix(Iterator(new LabeledPoint(0.0f, null, features.toDense.values.map(_.toFloat))))
    Vectors.dense(booster.predict(data, outPutMargin = true).head.map(_.toDouble))
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    throw new Exception("XGBoost Classification model does not support \'rawToProbabilityInPlace\'")
  }
}

case class XGBoostClassificationModel(impl: XGBoostClassificationModelBase) extends ProbabilisticClassificationModel {
  override val numClasses: Int = impl.numClasses
  override val numFeatures: Int = impl.numFeatures
  def booster: Booster = impl.booster

  def binaryClassificationModel: XGBoostBinaryClassificationModel = impl.asInstanceOf[XGBoostBinaryClassificationModel]
  def multinomialClassificationModel: XGBoostMultinomialClassificationModel = impl.asInstanceOf[XGBoostMultinomialClassificationModel]

  override def predictProbabilities(features: Vector): Vector = impl.predictProbabilities(features)
  override def predictRaw(features: Vector): Vector = impl.predictRaw(features)
  override def rawToProbabilityInPlace(raw: Vector): Vector = impl.rawToProbabilityInPlace(raw)
}
