package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.dmlc.xgboost4j.scala.DMatrix
import XgbConverters._

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class XGBoostRegression(override val uid: String = Transformer.uniqueName("xgboost.regression"),
                             override val shape: NodeShape,
                             override val model: XGBoostRegressionModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val prediction = (data: DMatrix) => Some(model.predict(data))
    val leafPrediction = shape.getOutput("leaf_prediction").map {
      _ => (data: DMatrix) => Some(model.predictLeaf(data))
    }.getOrElse((_: DMatrix) => None)
    val contribPrediction = shape.getOutput("contrib_prediction").map {
      _ => (data: DMatrix) => Some(model.predictContrib(data))
    }.getOrElse((_: DMatrix) => None)

    val all = Seq(prediction, leafPrediction, contribPrediction)

    val f = (features: Tensor[Double]) => {
      val data = features.asXGB
      val rowData = all.map(_.apply(data)).filter(_.isDefined).map(_.get)
      Row(rowData: _*)
    }

    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
