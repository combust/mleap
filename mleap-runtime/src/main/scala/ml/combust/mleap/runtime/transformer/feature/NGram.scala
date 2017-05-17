package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.NGramModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.runtime.types.{ListType, StringType, StructField}

import scala.util.{Success, Try}

/**
  * Created by mikhail on 10/16/16.
  */
case class NGram(override val uid: String = Transformer.uniqueName("ngram"),
                 override val inputCol: String,
                 override val outputCol: String,
                 model: NGramModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Seq[String]) => model(value)

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, ListType(StringType())),
    StructField(outputCol, ListType(StringType()))
  ))
}
