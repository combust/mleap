package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StopWordsRemoverModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.runtime.types.{ListType, StringType, StructField}

import scala.util.{Success, Try}

/**
  * Created by mikhail on 10/16/16.
  */
case class StopWordsRemover(override val uid:String = Transformer.uniqueName("stop_words_remover"),
                            override val inputCol: String,
                            override val outputCol: String,
                            model: StopWordsRemoverModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Seq[String]) => model(value)

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, ListType(StringType())),
    StructField(outputCol, ListType(StringType()))
  ))
}
