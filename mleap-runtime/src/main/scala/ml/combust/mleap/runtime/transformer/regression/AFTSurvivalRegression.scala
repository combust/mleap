package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.{MultiTransformer, Transformer}
import ml.combust.mleap.core.regression.AFTSurvivalRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.core.frame.Row

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class AFTSurvivalRegression(override val uid: String = Transformer.uniqueName("aft_survival_regression"),
                                 override val shape: NodeShape,
                                 override val model: AFTSurvivalRegressionModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val f = shape.getOutput("quantiles") match {
      case Some(_) =>
        (features: Tensor[Double]) => {
          val (prediction, quantiles) = model.predictWithQuantiles(features)
          Row(prediction, quantiles: Tensor[Double])
        }
      case None =>
        (features: Tensor[Double]) => Row(model(features))
    }

    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
