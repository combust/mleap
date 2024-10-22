package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.{HandleInvalid, StringIndexerModel}
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.{StructSelector, UserDefinedFunction}
import ml.combust.mleap.runtime.frame.{FrameBuilder, Row, Transformer}

import scala.util.Try

/**
 * Created by hwilkins on 10/22/15.
 */
case class StringIndexer(override val uid: String = Transformer.uniqueName("string_indexer"),
                         override val shape: NodeShape,
                         override val model: StringIndexerModel) extends Transformer with MultiInOutTransformer {
  private val outputs: Seq[String] = outputSchema.fields.map(_.name)
  private val inputs: Seq[String] = inputSchema.fields.map(_.name)
  private val inputSelector: StructSelector = StructSelector(inputs)
  private val filterSchema = StructType(Seq(StructField("output", ScalarType.Boolean.nonNullable))).get
  private val exec: UserDefinedFunction = UserDefinedFunction((keys: Row) => {
    val res = model(keys.toSeq)
    Row(res:_*)
  }, SchemaSpec(outputSchema), Seq(SchemaSpec(inputSchema)))
  private val contains: UserDefinedFunction = UserDefinedFunction((keys: Row) => {
    model.contains(keys.toSeq)
  }, SchemaSpec(filterSchema), Seq(SchemaSpec(inputSchema)))

  override def transform[FB <: FrameBuilder[FB]](builder: FB): Try[FB] = {
    def withColumns(builder: FB): Try[FB] = {
      builder.withColumns(outputs, inputSelector)(exec)
    }

    if(model.handleInvalid == HandleInvalid.Skip) {
      builder.filter(inputSelector)(contains)
        .flatMap(withColumns)
    } else {
      withColumns(builder)
    }
  }
}