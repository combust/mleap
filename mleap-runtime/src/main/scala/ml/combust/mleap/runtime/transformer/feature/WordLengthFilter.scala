package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.WordLengthFilterModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.runtime.types._

import scala.util.{Success, Try}

/**
  * Created by mageswarand on 14/2/17.
  */

case class WordLengthFilter(override val uid: String = Transformer.uniqueName("word_filter"),
                            override val inputCol: String,
                            override val outputCol: String,
                            model: WordLengthFilterModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (label: Seq[String]) => model(label)

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, ListType(StringType())),
    StructField(outputCol, ListType(StringType()))
  ))
}
