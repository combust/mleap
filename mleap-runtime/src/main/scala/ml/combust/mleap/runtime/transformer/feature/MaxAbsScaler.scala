package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.Tensor
import ml.combust.mleap.core.feature.MaxAbsScalerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

/**
  * Created by mikhail on 9/18/16.
  */
case class MaxAbsScaler(override val uid: String = Transformer.uniqueName("max_abs_scaler"),
                        override val inputCol: String,
                        override val outputCol: String,
                       model: MaxAbsScalerModel) extends FeatureTransformer {

  override val exec: UserDefinedFunction = (value: Tensor[Double]) => model(value): Tensor[Double]
}
