package ml.combust.mleap.xgboost

import biz.k11i.xgboost.Predictor
import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import ml.combust.mleap.tensor.Tensor

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class XGBoostRegressionModel(predictor: Predictor,
                                  booster: Option[Array[Byte]] = None,
                                  numFeatures: Int) extends Model {
  def predictDouble(tensor: Tensor[Double]): Double = {
    predictor.predictSingle(FVecTensorImpl(tensor))
  }

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Double).get
}
