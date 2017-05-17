package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CountVectorizerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.types._

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class CountVectorizer(override val uid: String = Transformer.uniqueName("count_vectorizer"),
                           override val inputCol: String,
                           override val outputCol: String,
                           model: CountVectorizerModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (document: Seq[String]) => model(document): Tensor[Double]

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, ListType(StringType())),
    StructField(outputCol, TensorType(DoubleType()))
  ))
}
