package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StopWordsRemoverModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

/**
  * Created by mikhail on 10/16/16.
  */
case class StopWordsRemover(override val uid:String = Transformer.uniqueName("stop_words_remover"),
                            override val inputCol: String,
                            override val outputCol: String,
                            model: StopWordsRemoverModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Array[String]) => model(value)
}
