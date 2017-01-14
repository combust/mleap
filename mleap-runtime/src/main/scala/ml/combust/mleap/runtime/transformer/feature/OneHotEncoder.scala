package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.Tensor
import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

import scala.util.Try

/**
  * Created by hollinwilkins on 5/10/16.
  */
case class OneHotEncoder(override val uid: String = Transformer.uniqueName("one_hot_encoder"),
                         override val inputCol: String,
                         override val outputCol: String,
                         model: OneHotEncoderModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Double) => model(value): Tensor[Double]
}
