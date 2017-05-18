package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.BisectingKMeansModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 12/26/16.
  */
case class BisectingKMeans(override val uid: String = Transformer.uniqueName("bisecting_k_means"),
                           featuresCol: String,
                           predictionCol: String,
                           model: BisectingKMeansModel) extends Transformer {
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(predictionCol, featuresCol)(exec)
  }

  override def getFields(): Try[Seq[StructField]] = Success(
    Seq(StructField(featuresCol, TensorType(DoubleType())),
      StructField(predictionCol, DoubleType())))
}
