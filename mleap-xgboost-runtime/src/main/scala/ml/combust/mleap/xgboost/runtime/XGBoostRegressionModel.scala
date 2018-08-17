package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import ml.combust.mleap.tensor.Tensor
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix}

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class XGBoostRegressionModel(booster: Booster,
                                  numFeatures: Int) extends Model {
  def predictDouble(tensor: Tensor[Double]): Double = {
    val data = new DMatrix(tensor.toDense.rawValues.map(_.toFloat), tensor.dimensions.head, 1)
    booster.predict(data).head(0)
  }

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Double.nonNullable).get
}
