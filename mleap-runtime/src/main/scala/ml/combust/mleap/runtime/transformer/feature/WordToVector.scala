package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.WordToVectorModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class WordToVector(override val uid: String = Transformer.uniqueName("word_to_vector"),
                        override val shape: NodeShape,
                        model: WordToVectorModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (sentence: Seq[String]) => model(sentence): Tensor[Double]
}
