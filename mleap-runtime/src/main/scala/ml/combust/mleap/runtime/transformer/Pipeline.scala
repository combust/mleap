package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{DataType, NodeShape, StructField, StructType}
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Created by hwilkins on 11/8/15.
 */
case class PipelineModel(transformers: Seq[Transformer]) extends Model {
  override def inputSchema: StructType = {
    throw new NotImplementedError("inputSchema is not implemented for a PipelineModel")
  }
  override def outputSchema: StructType = {
    throw new NotImplementedError("outputSchema is not implemented for a PipelineModel")
  }
}

case class Pipeline(override val uid: String = Transformer.uniqueName("pipeline"),
                    override val shape: NodeShape,
                    override val model: PipelineModel) extends Transformer {
  def transformers: Seq[Transformer] = model.transformers

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    model.transformers.foldLeft(Try(builder))((b, stage) => b.flatMap(stage.transform))
  }

  override def transformAsync[TB <: TransformBuilder[TB]](builder: TB)
                                                         (implicit ec: ExecutionContext): Future[TB] = {
    model.transformers.foldLeft(Future(builder)) {
      (fb, stage) => fb.flatMap(b => stage.transformAsync(b))
    }
  }

  override def close(): Unit = transformers.foreach(_.close())

  override def inputSchema: StructType = schemas._1
  override def outputSchema: StructType = schemas._2

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
}
