package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer}
import biz.k11i.xgboost.util.FVec
import XgbConverters._

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class XGBoostPerformantClassification(
                                            override val uid: String = Transformer.uniqueName("xgboost.classification"),
                                 override val shape: NodeShape,
                                 override val model: XGBoostPerformantClassificationModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val rawPrediction = shape.getOutput("raw_prediction").map {
      _ => (data: FVec) => Some(model.predictRaw(data): Tensor[Double])
    }.getOrElse((_: FVec) => None)
    val probability = shape.getOutput("probability").map {
      _ => (data: FVec) => Some(model.predictProbabilities(data): Tensor[Double])
    }.getOrElse((_: FVec) => None)
    val prediction = (data: FVec) => Some(model.predict(data))
    val leafPrediction = shape.getOutput("leaf_prediction").map {
      _ => (data: FVec) => Some(model.predictLeaf(data))
    }.getOrElse((_: FVec) => None)

    val all = Seq(rawPrediction, probability, prediction, leafPrediction)

    val f = (features: Tensor[Double]) => {
      val data = features.asXGBPredictor
      val rowData = all.map(_.apply(data)).filter(_.isDefined).map(_.get)
      Row(rowData: _*)
    }

    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
