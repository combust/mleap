package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.MultiLayerPerceptronClassifierModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.Try

/**
  * Created by hollinwilkins on 12/25/16.
  */
case class MultiLayerPerceptronClassifier(override val uid: String = Transformer.uniqueName("multi_layer_perceptron"),
                                          featuresCol: String,
                                          predictionCol: String,
                                          model: MultiLayerPerceptronClassifierModel) extends Transformer {
  val predict: UserDefinedFunction = (features: Tensor[Double]) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(predictionCol, featuresCol)(predict)
  }
}
