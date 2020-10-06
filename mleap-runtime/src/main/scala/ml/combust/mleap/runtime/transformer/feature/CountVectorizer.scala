package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CountVectorizerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class CountVectorizer(override val uid: String = Transformer.uniqueName("count_vectorizer"),
                           override val shape: NodeShape,
                           override val model: CountVectorizerModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (document: Seq[String]) => model(document)
}
