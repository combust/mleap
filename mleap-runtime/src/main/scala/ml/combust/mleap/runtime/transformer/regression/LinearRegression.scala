package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.core.types.{DoubleType, StructField, TensorType}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.{Success, Try}

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
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(predictionCol, featuresCol)(exec)
  }

  override def getFields(): Try[Seq[StructField]] = Success(
    Seq(StructField(featuresCol, TensorType(DoubleType())),
      StructField(predictionCol, DoubleType())))
}
