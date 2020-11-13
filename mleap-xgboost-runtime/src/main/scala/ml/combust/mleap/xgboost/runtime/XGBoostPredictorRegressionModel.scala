package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.Model
import biz.k11i.xgboost.Predictor
import biz.k11i.xgboost.util.FVec
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}


case class XGBoostPredictorRegressionModel(predictor: Predictor,
                                           numFeatures: Int,
                                           treeLimit: Int) extends Model {
  def predict(data: FVec): Double = predictor.predict(data, false, treeLimit).head

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Double.nonNullable).get
}
