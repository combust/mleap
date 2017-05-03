package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.IsotonicRegressionModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 12/27/16.
  */
case class IsotonicRegression(override val uid: String = Transformer.uniqueName("isotonic_regression"),
                              featuresCol: String,
                              predictionCol: String,
                              model: IsotonicRegressionModel) extends Transformer {
  val execIndexed: UserDefinedFunction = (features: Tensor[Double]) => model(features)
  val exec: UserDefinedFunction = (feature: Double) => model(feature)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    model.featureIndex match {
      case Some(index) => builder.withOutput(predictionCol, featuresCol)(execIndexed)
      case None => builder.withOutput(predictionCol, featuresCol)(exec)
    }
  }

  override def getSchema(): Try[Seq[StructField]] = {
    model.featureIndex match {
      case Some(index) => Success(Seq(
        StructField(featuresCol, TensorType(DoubleType())),
        StructField(predictionCol, DoubleType())))
      case None => Success(Seq(
        StructField(featuresCol, DoubleType()),
        StructField(predictionCol, DoubleType())))
    }
  }
}
