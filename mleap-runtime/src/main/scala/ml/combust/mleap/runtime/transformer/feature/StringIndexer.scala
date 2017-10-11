package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.{HandleInvalid, StringIndexerModel}
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.{FieldSelector, UserDefinedFunction}
import ml.combust.mleap.runtime.frame.{FrameBuilder, Transformer}

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class StringIndexer(override val uid: String = Transformer.uniqueName("string_indexer"),
                         override val shape: NodeShape,
                         override val model: StringIndexerModel) extends Transformer {
  val input: String = inputSchema.fields.head.name
  val inputSelector: FieldSelector = input
  val output: String = outputSchema.fields.head.name
  val exec: UserDefinedFunction = (value: String) => model(value).toDouble

  override def transform[FB <: FrameBuilder[FB]](builder: FB): Try[FB] = {
    if(model.handleInvalid == HandleInvalid.Skip) {
      builder.filter(input) {
        (key: String) => model.stringToIndex.contains(key)
      }.flatMap(_.withColumn(output, inputSelector)(exec))
    } else {
      builder.withColumn(output, inputSelector)(exec)
    }
  }
}
