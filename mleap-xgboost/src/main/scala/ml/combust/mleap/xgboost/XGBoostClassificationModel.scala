package ml.combust.mleap.xgboost

import biz.k11i.xgboost.Predictor
import ml.combust.mleap.core.classification.ProbabilisticClassificationModel
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

trait XGBoostClassificationModelBase extends ProbabilisticClassificationModel {
  val booster: Option[Array[Byte]]
  val predictor: Predictor
  val outputMargin: Boolean
}

case class XGBoostBinaryClassificationModel(override val predictor: Predictor,
                                            override val booster: Option[Array[Byte]],
                                            override val numFeatures: Int,
                                            override val outputMargin: Boolean) extends XGBoostClassificationModelBase {
  override val numClasses: Int = 2

  override def predictRaw(features: Vector): Vector = {
    val m = predictor.predictSingle(FVecVectorImpl(features), outputMargin)
    Vectors.dense(1 - m, m)
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = raw
}

case class XGBoostMultinomialClassificationModel(override val predictor: Predictor,
                                                 override val booster: Option[Array[Byte]],
                                                 override val numClasses: Int,
                                                 override val numFeatures: Int,
                                                 override val outputMargin: Boolean) extends XGBoostClassificationModelBase {
  override def predictRaw(features: Vector): Vector = {
    Vectors.dense(predictor.predict(FVecVectorImpl(features), outputMargin))
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    raw match {
      case dv: DenseVector =>
        val max = dv.values.max
        val values = dv.values.map(v => Math.exp(v - max)).array
        val sum = values.sum
        Vectors.dense(values.map(_ / sum).array)
      case _: SparseVector =>
        throw new Exception("rawPrediction should be DenseVector")
    }
  }
}

case class XGBoostClassificationModel(impl: XGBoostClassificationModelBase) extends ProbabilisticClassificationModel {
  def outputMargin: Boolean = impl.outputMargin
  override val numClasses: Int = impl.numClasses
  override val numFeatures: Int = impl.numFeatures
  def booster: Option[Array[Byte]] = impl.booster

  def binaryClassificationModel: XGBoostBinaryClassificationModel = impl.asInstanceOf[XGBoostBinaryClassificationModel]
  def multinomialClassificationModel: XGBoostMultinomialClassificationModel = impl.asInstanceOf[XGBoostMultinomialClassificationModel]

  override def predictRaw(features: Vector): Vector = impl.predictRaw(features)
  override def rawToProbabilityInPlace(raw: Vector): Vector = impl.rawToProbabilityInPlace(raw)
}
