package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ReverseStringIndexerModel
import ml.combust.mleap.core.types.{ListShape, NodeShape, ScalarShape}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class ReverseStringIndexer(override val uid: String = Transformer.uniqueName("reverse_string_indexer"),
                                override val shape: NodeShape,
                                override val model: ReverseStringIndexerModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = model.inputShape match {
    case ScalarShape(false) => (value: Double) => model(value.toInt)
    case ListShape(false) => (value: Seq[Double]) => model(value.map(_.toInt))
  }
}
