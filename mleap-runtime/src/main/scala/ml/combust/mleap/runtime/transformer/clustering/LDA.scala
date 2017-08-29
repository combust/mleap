package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.LocalLDAModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor

/**
  * Created by mageswarand on 3/3/17.
  */
case class LDA(override val uid: String = Transformer.uniqueName("lda"),
               override val shape: NodeShape,
               override val model: LocalLDAModel) extends SimpleTransformer {

  override val exec: UserDefinedFunction = (features: Tensor[Double]) => model.topicDistribution(features): Tensor[Double]
}
