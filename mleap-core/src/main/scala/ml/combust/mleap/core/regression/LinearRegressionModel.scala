package ml.combust.mleap.core.regression

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.mleap.BLAS

/** Class for linear regression model.
  *
  * @param coefficients coefficients for linear regression
  * @param intercept intercept for regression
  */
case class LinearRegressionModel(coefficients: Vector,
                                 intercept: Double) extends Model {
  /** Alias for [[ml.combust.mleap.core.regression.LinearRegressionModel#predict]]
    *
    * @param features features for prediction
    * @return prediction
    */
  def apply(features: Vector): Double = predict(features)

  /** Predict a value using this linear regression.
    *
    * @param features features for prediction
    * @return prediction
    */
  def predict(features: Vector): Double = {
    BLAS.dot(features, coefficients) + intercept
  }

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(coefficients.size)).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Double.nonNullable).get
}
