package ml.combust.mleap.xgboost

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{MultiTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class XGBoostClassification(override val uid: String = Transformer.uniqueName("xgboost.classification"),
                                 override val shape: NodeShape,
                                 override val model: XGBoostClassificationModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val f = shape.getOutput("raw_prediction") match {
      case Some(_) =>
        (features: Tensor[Double]) => {
          val rawPrediction = model.predictRaw(features)
          val prediction = model.rawToPrediction(rawPrediction)
          Row(rawPrediction: Tensor[Double], prediction)
        }
      case None =>
        (features: Tensor[Double]) => Row(model(features))
    }

    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
