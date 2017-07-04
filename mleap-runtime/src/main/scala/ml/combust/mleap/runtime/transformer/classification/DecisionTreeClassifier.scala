package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.DecisionTreeClassifierModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{MultiTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.Row

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class DecisionTreeClassifier(override val uid: String = Transformer.uniqueName("decision_tree_classification"),
                                  override val shape: NodeShape,
                                  model: DecisionTreeClassifierModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    (shape.getOutput("raw_prediction"), shape.getOutput("probability")) match {
      case (Some(_), Some(_)) =>
        (features: Tensor[Double]) => {
          val rawPrediction = model.predictRaw(features)
          val probability = model.rawToProbability(rawPrediction)
          val prediction = model.predictWithProbability(probability)
          Row(prediction, rawPrediction: Tensor[Double], probability: Tensor[Double])
        }
      case (Some(_), None) =>
        (features: Tensor[Double]) => {
          val rawPrediction = model.predictRaw(features)
          val prediction = model.rawToPrediction(rawPrediction)
          Row(prediction, rawPrediction: Tensor[Double])
        }
      case (None, Some(_)) =>
        (features: Tensor[Double]) => {
          val probability = model.predictProbabilities(features)
          val prediction = model.probabilityToPrediction(probability)
          Row(prediction, probability: Tensor[Double])
        }
      case (None, None) =>
        (features: Tensor[Double]) => Row(model(features))
    }
  }
}
