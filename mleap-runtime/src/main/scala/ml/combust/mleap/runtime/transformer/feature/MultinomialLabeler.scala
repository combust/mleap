package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MultinomialLabelerModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.frame.{MultiTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor

/**
  * Created by hollinwilkins on 1/18/17.
  */
case class MultinomialLabeler(override val uid: String = Transformer.uniqueName("multinomial_labeler"),
                              override val shape: NodeShape,
                              override val model: MultinomialLabelerModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = (t: Tensor[Double]) => {
    val outs = model(t)
    val probabilities = outs.map(_._1)
    val labels = outs.map(_._3)

    (probabilities, labels)
  }
}
