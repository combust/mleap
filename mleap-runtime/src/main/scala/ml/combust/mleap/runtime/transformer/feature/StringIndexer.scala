package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}

/**
  * Created by hwilkins on 10/22/15.
  */
case class StringIndexer(override val uid: String = Transformer.uniqueName("string_indexer"),
                         override val shape: NodeShape,
                         override val model: StringIndexerModel) extends SimpleTransformer {
  val exec: UserDefinedFunction = (value: String) => model(value).toDouble
}
