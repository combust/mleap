package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.NGramModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

/**
  * Created by mikhail on 10/16/16.
  */
case class NGram(override val uid: String = Transformer.uniqueName("ngram"),
                 override val inputCol: String,
                 override val outputCol: String,
                 model: NGramModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Seq[String]) => model(value)
}
