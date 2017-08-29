package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.GBTClassifierModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.transformer.{MultiTransformer, Transformer}
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor

/**
  * Created by hollinwilkins on 9/24/16.
  */
case class GBTClassifier(override val uid: String = Transformer.uniqueName("gbt_classifier"),
                         override val shape: NodeShape,
                         override val model: GBTClassifierModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val f = (shape.getOutput("raw_prediction"), shape.getOutput("probability")) match {
      case (Some(_), Some(_)) =>
        (features: Tensor[Double]) => {
          val rawPrediction = model.predictRaw(features)
          val probability = model.rawToProbability(rawPrediction)
          val prediction = model.probabilityToPrediction(probability)
          Row(rawPrediction: Tensor[Double], probability: Tensor[Double], prediction)
        }
      case (Some(_), None) =>
        (features: Tensor[Double]) => {
          val rawPrediction = model.predictRaw(features)
          val prediction = model.rawToPrediction(rawPrediction)
          Row(rawPrediction: Tensor[Double], prediction)
        }
      case (None, Some(_)) =>
        (features: Tensor[Double]) => {
          val probability = model.predictProbabilities(features)
          val prediction = model.probabilityToPrediction(probability)
          Row(probability: Tensor[Double], prediction)
        }
      case (None, None) =>
        (features: Tensor[Double]) => Row(model(features))
    }

    UserDefinedFunction(f, outputSchema, inputSchema)
  }}
