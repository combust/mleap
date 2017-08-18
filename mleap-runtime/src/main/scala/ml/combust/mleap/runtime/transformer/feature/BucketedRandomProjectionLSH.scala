package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BucketedRandomProjectionLSHModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.SchemaUtil

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class BucketedRandomProjectionLSH(override val uid: String = Transformer.uniqueName("bucketed_rp_lsh"),
                                       override val shape: NodeShape,
                                       override val model: BucketedRandomProjectionLSHModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)

  SchemaUtil.asssertModelTypesMatchTransformerTypes(model, exec)
}
