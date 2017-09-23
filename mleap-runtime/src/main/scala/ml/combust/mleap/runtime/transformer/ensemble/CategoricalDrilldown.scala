package ml.combust.mleap.runtime.transformer.ensemble

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.{RowTransformBuilder, TransformBuilder}

import scala.util.Try

/**
  * Created by hollinwilkins on 9/22/17.
  */
case class CategoricalDrilldownModel(transformers: Map[String, Transformer]) extends Model {
  override def inputSchema: StructType = transformers.head._2.model.inputSchema
  override def outputSchema: StructType = transformers.head._2.model.outputSchema

  val transformerInputSchema: StructType = transformers.head._2.inputSchema
  val transformerOutputSchema: StructType = transformers.head._2.outputSchema

  private val rowTransformers: Map[String, RowTransformBuilder] = {
    transformers.map {
      case (label, transformer) =>
        (label, transformer.transform(RowTransformBuilder(transformerInputSchema))).get
    }
  }
  private val outputIndices: Map[String, Seq[Int]] = {
    rowTransformers.map {
      case (label, rowTransformer) =>
        val indices = transformerOutputSchema.fields.map(_.name).map {
          name => rowTransformer.outputSchema.indexOf(name).get
        }
        (label, indices)
    }
  }

  def apply(label: String, row: Row): Row = {
    rowTransformers(label).transform(row).selectIndices(outputIndices(label): _*)
  }
}

case class CategoricalDrilldown(override val uid: String,
                                override val shape: NodeShape,
                                override val model: CategoricalDrilldownModel) extends Transformer {
  private val f = (label: String, row: Row) => model(label, row)
  val exec: UserDefinedFunction = UserDefinedFunction(f,
    outputSchema,
    Seq(DataTypeSpec(ScalarType.String), SchemaSpec(inputSchema)))

  val outputCols: Seq[String] = shape.outputs.values.map(_.name).toSeq
  val inputSelectors: Seq[Selector] = Seq(inputSchema.fields.head.name, inputSchema.fields.tail.map(_.name))

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutputs(outputCols, inputSelectors: _*)(exec)
  }
}
