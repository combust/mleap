package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.LightGBMRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor

case class LightGBMRegression(override val uid: String = Transformer.uniqueName("lightgbm_regression"),
                              override val shape: NodeShape,
                              override val model: LightGBMRegressionModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
      val f = (features: Tensor[Double]) => {
        val prediction = model.predict(features)
        Row(prediction: Double)
    }
    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
