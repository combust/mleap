package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.WordLengthFilterModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

/**
  * Created by mageswarand on 14/2/17.
  * https://github.com/combust/mleap/blob/master/mleap-runtime/src/main/scala/ml/combust/mleap/runtime/transformer/feature/Tokenizer.scala
  */

case class WordLengthFilter(override val uid: String = Transformer.uniqueName("word_filter"),
                            override val inputCol: String,
                            override val outputCol: String,
                            model: WordLengthFilterModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (label: Seq[String]) => model(label)
}
