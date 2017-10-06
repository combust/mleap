package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.{MultiTransformer, Transformer}
import ml.combust.mleap.core.clustering.GaussianMixtureModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.core.frame.Row

/**
  * Created by hollinwilkins on 11/17/16.
  */
case class GaussianMixture(override val uid: String = Transformer.uniqueName("gmm"),
                           override val shape: NodeShape,
                           override val model: GaussianMixtureModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val f = shape.getOutput("probability") match {
      case Some(_) =>
        (features: Tensor[Double]) => {
          val probability = model.predictProbability(features)
          val prediction = model.predictionFromProbability(probability)
          Row(prediction, probability: Tensor[Double])
        }
      case None =>
        (features: Tensor[Double]) => Row(model(features))
    }

    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
