package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.HashingTermFrequencyModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hwilkins on 12/30/15.
  */
case class HashingTermFrequency(override val uid: String = Transformer.uniqueName("hashing_term_frequency"),
                                override val shape: NodeShape,
                                override val model: HashingTermFrequencyModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (value: Seq[String]) => model(value): Tensor[Double]
}
