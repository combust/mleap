package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.DoubleType

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class StringIndexer(uid: String = Transformer.uniqueName("string_indexer"),
                         inputCol: String,
                         outputCol: String,
                         model: StringIndexerModel) extends Transformer {
  val exec: UserDefinedFunction = (value: Any) => model(value.toString)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, inputCol)(exec)
  }

  def toReverse: ReverseStringIndexer = ReverseStringIndexer(inputCol = inputCol,
    outputCol = outputCol,
    model = model.toReverse)

  def toReverse(name: String): ReverseStringIndexer = ReverseStringIndexer(uid = name,
    inputCol = inputCol,
    outputCol = outputCol,
    model = model.toReverse)
}
