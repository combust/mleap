package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import biz.k11i.xgboost.util.FVec
import XgbConverters._


case class XGBoostPerformantClassification(
                                  override val uid: String = Transformer.uniqueName("xgboost.classification"),
                                 override val shape: NodeShape,
                                 override val model: XGBoostPerformantClassificationModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val rawPrediction = shape.getOutput("raw_prediction").map {
      _ => (data: FVec) => Some(model.predictRaw(data): Vector)
    }.getOrElse((_: FVec) => None)

    val prediction = (data: FVec) => Some(model.predict(data))

    val all = Seq(prediction)

    val f = (features: Tensor[Double]) => {
      val data: FVec = features.asXGBPredictor
      val rowData = all.map(_.apply(data)).filter(_.isDefined).map(_.get)
      Row(rowData: _*)
    }

    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
