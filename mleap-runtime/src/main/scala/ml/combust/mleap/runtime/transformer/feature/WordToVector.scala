package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.WordToVectorModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class WordToVector(override val uid: String = Transformer.uniqueName("word_to_vector"),
                        override val inputCol: String,
                        override val outputCol: String,
                        model: WordToVectorModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (sentence: Seq[String]) => model(sentence): Tensor[Double]
}
