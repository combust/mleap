package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import ml.combust.mleap.tensor.Tensor
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix}
import XgbConverters._

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class XGBoostRegressionModel(booster: Booster,
                                  numFeatures: Int,
                                  treeLimit: Int,
                                  missing: Float = Float.NaN) extends Model {
  def predict(tensor: Tensor[Double]): Double = predict(tensor.asXGB(missing))
  def predict(data: DMatrix): Double = booster.predict(data, outPutMargin = false, treeLimit = treeLimit).head(0)

  def predictLeaf(tensor: Tensor[Double]): Tensor[Double] = predictLeaf(tensor.asXGB(missing))
  def predictLeaf(data: DMatrix): Tensor[Double] = Tensor.denseVector(booster.predictLeaf(data, treeLimit = treeLimit).head.map(_.toDouble))

  def predictContrib(tensor: Tensor[Double]): Tensor[Double] = predictContrib(tensor.asXGB(missing))
  def predictContrib(data: DMatrix): Tensor[Double] = Tensor.denseVector(booster.predictContrib(data, treeLimit = treeLimit).head.map(_.toDouble))

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Double.nonNullable,
    "leaf_prediction" -> TensorType.Double(),
    "contrib_prediction" -> TensorType.Double()).get
}
