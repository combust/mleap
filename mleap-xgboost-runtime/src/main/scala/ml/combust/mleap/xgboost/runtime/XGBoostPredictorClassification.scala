package ml.combust.mleap.xgboost.runtime

import biz.k11i.xgboost.util.FVec
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer}
import ml.combust.mleap.core.util.VectorConverters._
import XgbConverters._


case class XGBoostPredictorClassification(
                                  override val uid: String = Transformer.uniqueName("xgboost.classification"),
                                 override val shape: NodeShape,
                                 override val model: XGBoostPredictorClassificationModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {

    // Since the Predictor is our performant implementation, we only compute probability for performance reasons.
    val probability = shape.getOutput("probability").map {
      _ => (data: FVec) => Some(model.predictProbabilities(data): Tensor[Double])
    }.getOrElse((_: FVec) => None)

    val f = (features: Tensor[Double]) => {
      val data: FVec = features.asXGBPredictor
      val rowData = Seq(probability(data).get)
      Row(rowData: _*)
    }

    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
