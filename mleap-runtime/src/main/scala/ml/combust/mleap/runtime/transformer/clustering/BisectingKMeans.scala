package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.BisectingKMeansModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hollinwilkins on 12/26/16.
  */
case class BisectingKMeans(override val uid: String = Transformer.uniqueName("bisecting_k_means"),
                           override val shape: NodeShape,
                           override val model: BisectingKMeansModel) extends SimpleTransformer {
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)
}
