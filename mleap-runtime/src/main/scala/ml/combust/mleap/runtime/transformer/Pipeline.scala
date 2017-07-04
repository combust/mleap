package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.core.types.{DataType, NodeShape, StructField, StructType}
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
 * Created by hwilkins on 11/8/15.
 */
case class Pipeline(override val uid: String = Transformer.uniqueName("pipeline"),
                    transformers: Seq[Transformer]) extends Transformer {
  override val shape: NodeShape = NodeShape()

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    transformers.foldLeft(Try(builder))((b, stage) => b.flatMap(stage.transform))
  }

  override lazy val inputSchema: StructType = schemas._1
  override lazy val outputSchema: StructType = schemas._2

  private lazy val schemas: (StructType, StructType) = {
    val (inputs, outputs) = transformers.foldLeft((Map[String, DataType](), Map[String, DataType]())) {
      case ((iacc, oacc), tform) =>
        (iacc ++ tform.inputSchema.fields.map(f => f.name -> f.dataType),
          oacc ++ tform.outputSchema.fields.map(f => f.name -> f.dataType))
    }

    val actualInputs = (inputs -- outputs.keys).map {
      case (name, dt) => StructField(name, dt)
    }.toSeq

    val actualOutputs = outputs.map {
      case (name, dt) => StructField(name, dt)
    }.toSeq

    (StructType(actualInputs).get, StructType(actualOutputs).get)
  }

  override def close(): Unit = transformers.foreach(_.close())
}
