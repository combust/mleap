package ml.combust.mleap.core.regression

import com.microsoft.ml.spark.lightgbm.LightGBMBooster
import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import org.apache.spark.ml.linalg.{Vector, Vectors}

object LightGBMRegressionModel{
  def apply(model: String,
            featuresColName: String,
            predictionColName: String): LightGBMRegressionModel = LightGBMRegressionModel(
    model, featuresColName, predictionColName)
}

case class LightGBMRegressionModel(
                         override val booster: LightGBMBooster,
                         override val featuresColName: String,
                         override val predictionColName: String)
  extends LightGBMRegressionModelBase with Model {

  override def inputSchema: StructType = StructType("features" -> TensorType.Double()).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Double.nonNullable).get

  def predict(features: Vector): Double = {
    booster.score(features, false, false)(0)
  }
}

trait LightGBMRegressionModelBase {
  def booster: LightGBMBooster
  def featuresColName: String
  def predictionColName: String
}
