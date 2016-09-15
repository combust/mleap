package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, TensorType}

import scala.util.Try

/** Class for an MLeap linear regression transformer.
  *
  * @param uid unique identifier
  * @param featuresCol input column containing features
  * @param predictionCol output column containing prediction
  * @param model linear regression model
  */
case class LinearRegression(uid: String = Transformer.uniqueName("linear_regression"),
                            featuresCol: String,
                            predictionCol: String,
                            model: LinearRegressionModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(featuresCol, TensorType.doubleVector()).flatMap {
      case(b, featuresIndex) =>
        b.withOutput(predictionCol, DoubleType)(row => model(row.getVector(featuresIndex)))
    }
  }
}
