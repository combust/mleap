package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BucketedRandomProjectionLSHModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.FeatureTransformer
import org.apache.spark.ml.linalg.Vector

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class BucketedRandomProjectionLSH(override val uid: String,
                                       override val inputCol: String,
                                       override val outputCol: String,
                                       model: BucketedRandomProjectionLSHModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (features: Vector) => model(features)
}
