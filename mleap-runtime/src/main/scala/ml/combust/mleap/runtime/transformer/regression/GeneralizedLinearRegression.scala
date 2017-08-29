package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.GeneralizedLinearRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{MultiTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.Row

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class GeneralizedLinearRegression(override val uid: String = Transformer.uniqueName("generalized_lr"),
                                       override val shape: NodeShape,
                                       override val model: GeneralizedLinearRegressionModel) extends MultiTransformer {


  override val exec: UserDefinedFunction = {
    val f = shape.getOutput("link_prediction") match {
      case Some(_) =>
        (features: Tensor[Double]) => {
          val (prediction, link) = model.predictWithLink(features)
          Row(prediction, link)
        }
      case None =>
        (features: Tensor[Double]) => Row(model(features))
    }

    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
