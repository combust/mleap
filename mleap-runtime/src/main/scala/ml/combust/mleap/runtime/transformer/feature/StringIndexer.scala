package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

/**
  * Created by hwilkins on 10/22/15.
  */
case class StringIndexer(override val uid: String = Transformer.uniqueName("string_indexer"),
                         override val inputCol: String,
                         override val outputCol: String,
                         model: StringIndexerModel) extends FeatureTransformer {
  val exec: UserDefinedFunction = (value: Any) => model(value)

  def toReverse: ReverseStringIndexer = ReverseStringIndexer(inputCol = inputCol,
    outputCol = outputCol,
    model = model.toReverse)

  def toReverse(name: String): ReverseStringIndexer = ReverseStringIndexer(uid = name,
    inputCol = inputCol,
    outputCol = outputCol,
    model = model.toReverse)
}
