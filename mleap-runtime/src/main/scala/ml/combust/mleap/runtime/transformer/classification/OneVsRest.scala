package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.OneVsRestModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer}

/**
  * Created by hwilkins on 10/22/15.
  */
case class OneVsRest(override val uid: String = Transformer.uniqueName("one_vs_rest"),
                     override val shape: NodeShape,
                     override val model: OneVsRestModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val f = (shape.getOutput("raw_prediction"), shape.getOutput("probability")) match {
      case (Some(_), Some(_)) =>
        (features: Tensor[Double]) => {
          val (probability, rawPrediction, prediction) = model.predictAll(features)
          Row(probability, rawPrediction: Tensor[Double], prediction)
        }
      case (None, Some(_)) =>
        (features: Tensor[Double]) => {
          val (probability, _, prediction) = model.predictAll(features)
          Row(probability, prediction)
        }

      case (Some(_), None) =>
        (features: Tensor[Double]) => {
          val (_, rawPrediction, prediction) = model.predictAll(features)
          Row(rawPrediction: Tensor[Double], prediction)
        }

      case (None, None) =>
        (features: Tensor[Double]) => Row(model(features))
    }

    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
