package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.Model
import com.yelp.xgboost.Predictor
import com.yelp.xgboost.FVec
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}


case class XGBoostPredictorRegressionModel(predictor: Predictor,
                                           numFeatures: Int,
                                           treeLimit: Int,
                                           treatsZeroAsNA: Boolean = false) extends Model {
  def predict(data: FVec): Double = predictor.predict(data, treeLimit).head

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Double.nonNullable).get
}
