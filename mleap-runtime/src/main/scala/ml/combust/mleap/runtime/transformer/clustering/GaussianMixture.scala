package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.GaussianMixtureModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.types.{DoubleType, IntegerType, StructField, TensorType}

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 11/17/16.
  */
case class GaussianMixture(override val uid: String = Transformer.uniqueName("gmm"),
                           featuresCol: String,
                           predictionCol: String,
                           probabilityCol: Option[String] = None,
                           model: GaussianMixtureModel) extends Transformer {
  val predictProbability: UserDefinedFunction = (features: Tensor[Double]) => model.predictProbability(features): Tensor[Double]
  val predictionFromProbability: UserDefinedFunction = (features: Tensor[Double]) => model.predictionFromProbability(features)
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    probabilityCol match {
      case Some(probability) =>
        for(b <- builder.withOutput(probability, featuresCol)(predictProbability);
            b2 <- b.withOutput(predictionCol, probability)(predictionFromProbability)) yield b2
      case None => builder.withOutput(predictionCol, featuresCol)(exec)
    }
  }

  override def getSchema(): Try[Seq[StructField]] = {
    probabilityCol match {
      case Some(probability) => Success(Seq(
        StructField(featuresCol, TensorType(DoubleType())),
        StructField(predictionCol, IntegerType()),
        StructField(probability, TensorType(DoubleType()))))
      case None => Success(Seq(
        StructField(featuresCol, TensorType(DoubleType())),
        StructField(predictionCol, IntegerType())))
    }
  }
}
