package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.WordToVectorModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.types._

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class WordToVector(override val uid: String = Transformer.uniqueName("word_to_vector"),
                        override val inputCol: String,
                        override val outputCol: String,
                        model: WordToVectorModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (sentence: Seq[String]) => model(sentence): Tensor[Double]

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, ListType(StringType())),
    StructField(outputCol, TensorType(DoubleType()))
  ))
}
