package ml.combust.mleap.xgboost

import biz.k11i.xgboost.Predictor
import ml.combust.mleap.core.classification.ProbabilisticClassificationModel
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class XGBoostClassificationModel(predictor: Predictor,
                                      outputMargin: Boolean) extends ProbabilisticClassificationModel {
  override val numClasses: Int = 2
  override val numFeatures: Int = -1

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
