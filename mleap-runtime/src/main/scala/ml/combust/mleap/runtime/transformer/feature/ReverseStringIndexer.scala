package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ReverseStringIndexerModel
import ml.combust.mleap.core.types.{ListShape, NodeShape, ScalarShape, TensorShape}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class ReverseStringIndexer(override val uid: String = Transformer.uniqueName("reverse_string_indexer"),
                                override val shape: NodeShape,
                                override val model: ReverseStringIndexerModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = model.inputShape match {
    case ScalarShape(false) => (value: Double) => model(value.toInt)
    case ListShape(false) => (value: Seq[Double]) => model(value.map(_.toInt))
    case TensorShape(_, false) => (value: Tensor[Double]) => model(value.mapValues(_.toInt))
    case _ => throw new IllegalArgumentException(s"invalid shape for reverse string indexer ${model.inputShape}")
  }
}
