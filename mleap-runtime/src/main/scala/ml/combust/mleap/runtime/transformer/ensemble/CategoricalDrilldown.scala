package ml.combust.mleap.runtime.transformer.ensemble

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{FrameBuilder, RowTransformer, Transformer, Row}
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}

import scala.util.Try

/**
  * Created by hollinwilkins on 9/22/17.
  */
case class CategoricalDrilldownModel(transformers: Map[String, Transformer]) extends Model {
  override def inputSchema: StructType = StructType(StructField("label", ScalarType.String.nonNullable) +: transformers.head._2.model.inputSchema.fields.map {
    f => f.copy(name = s"drilldown.${f.name}")
  }).get

  override def outputSchema: StructType = StructType(transformers.head._2.model.outputSchema.fields.map {
    f => f.copy(name = s"drilldown.${f.name}")
  }).get

  private val rowTransformers: Map[String, (Transformer, RowTransformer)] = {
    transformers.map {
      case (label, transformer) =>
        (label, (transformer, transformer.transform(RowTransformer(transformer.inputSchema)).get))
    }
  }

  private val outputIndices: Map[String, Seq[Int]] = {
    rowTransformers.map {
      case (label, (transformer, rowTransformer)) =>
        val indices = transformer.outputSchema.fields.map(_.name).map {
          name => rowTransformer.outputSchema.indexOf(name).get
        }
        (label, indices)
    }
  }

  def apply(label: String, row: Row): Row = {
    rowTransformers(label)._2.transform(row).selectIndices(outputIndices(label): _*)
  }
}

case class CategoricalDrilldown(override val uid: String = Transformer.uniqueName("categorical_drilldown"),
                                override val shape: NodeShape,
                                override val model: CategoricalDrilldownModel) extends Transformer {
  private val f = (label: String, row: Row) => model(label, row)
  val exec: UserDefinedFunction = UserDefinedFunction(f,
    outputSchema,
    Seq(DataTypeSpec(ScalarType.String.nonNullable), SchemaSpec(StructType(inputSchema.fields.tail).get)))

  val outputCols: Seq[String] = shape.outputs.values.map(_.name).toSeq
  val inputSelectors: Seq[Selector] = Seq(inputSchema.fields.head.name, inputSchema.fields.tail.map(_.name))

  override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumns(outputCols, inputSelectors: _*)(exec)
  }
}
