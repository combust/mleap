package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.AFTSurvivalRegressionModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class AFTSurvivalRegression(override val uid: String = Transformer.uniqueName("aft_survival_regression"),
                                 featuresCol: String,
                                 predictionCol: String,
                                 quantilesCol: Option[String] = None,
                                 model: AFTSurvivalRegressionModel) extends Transformer {
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model.predict(features)
  val execQuantiles: UserDefinedFunction = (features: Tensor[Double]) => model.predictQuantiles(features): Tensor[Double]

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    quantilesCol match {
      case Some(col) =>
        for(b1 <- builder.withOutput(predictionCol, featuresCol)(exec);
            b2 <- b1.withOutput(col, featuresCol)(execQuantiles)) yield b2
      case None => builder.withOutput(predictionCol, featuresCol)(exec)
    }
  }

  override def getFields(): Try[Seq[StructField]] = {
    quantilesCol match {
      case Some(col) =>
        Success(Seq(StructField(featuresCol, TensorType(DoubleType())),
          StructField(predictionCol, DoubleType()),
          StructField(col, TensorType(DoubleType()))))
      case None => Success(
        Seq(StructField(featuresCol, TensorType(DoubleType())),
        StructField(predictionCol, DoubleType())))
    }
  }
}
