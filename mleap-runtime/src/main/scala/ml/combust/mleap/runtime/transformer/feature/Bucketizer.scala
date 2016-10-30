package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BucketizerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

/**
  * Created by mikhail on 9/19/16.
  */
case class Bucketizer(override val uid: String = Transformer.uniqueName("bucketizer"),
                      override val inputCol: String,
                      override val outputCol: String,
                      model: BucketizerModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Double) => model(value)
}
