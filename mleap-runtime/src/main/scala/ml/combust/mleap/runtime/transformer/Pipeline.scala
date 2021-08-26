package ml.combust.mleap.runtime.transformer

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{DataType, NodeShape, StructField, StructType}
import ml.combust.mleap.runtime.frame.{FrameBuilder, Transformer => FrameTransformer}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Created by hwilkins on 11/8/15.
  */
case class PipelineModel(transformers: Seq[FrameTransformer]) extends Model {
  override def inputSchema: StructType = {
    throw new NotImplementedError("inputSchema is not implemented for a PipelineModel")
  }
  override def outputSchema: StructType = {
    throw new NotImplementedError("outputSchema is not implemented for a PipelineModel")
  }
}

case class Pipeline(override val uid: String = FrameTransformer.uniqueName("pipeline"),
                    override val shape: NodeShape,
                    override val model: PipelineModel) extends FrameTransformer {
  def transformers: Seq[FrameTransformer] = model.transformers

  override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    model.transformers.foldLeft(Try(builder))((b, stage) => b.flatMap(stage.transform))
  }

  override def transformAsync[FB <: FrameBuilder[FB]](builder: FB)
                                                     (implicit ec: ExecutionContext): Future[FB] = {
    model.transformers.foldLeft(Future(builder)) {
      (fb, stage) => fb.flatMap(b => stage.transformAsync(b))
    }
  }

  override def close(): Unit = transformers.foreach(_.close())

  override def inputSchema: StructType = schemas._1
  override def outputSchema: StructType = schemas._2
  def intermediateSchema: StructType = schemas._3
  def strictOutputSchema: StructType = schemas._4

  private lazy val schemas: (StructType, StructType, StructType, StructType) = {
    val (inputs, outputs, intermediates) = transformers.foldLeft(
      (Map[String, DataType](), Map[String, DataType](), Map[String, DataType]())) {
      case ((iacc, oacc, intacc), tform) =>
        (iacc ++ tform.inputSchema.fields.map(f => f.name -> f.dataType),
          oacc ++ tform.outputSchema.fields.map(f => f.name -> f.dataType),
          intacc ++ { tform match {
            case pip: Pipeline => pip.intermediateSchema.fields.map(f => f.name -> f.dataType)
            case _ => Map() } })
    }

    val actualInputs = (inputs -- outputs.keys).map {
      case (name, dt) => StructField(name, dt)
    }.toSeq

    val actualOutputs = outputs.map {
      case (name, dt) => StructField(name, dt)
    }.toSeq

    val strictOutputs = outputs -- inputs.keys -- intermediates.keys
    val strictOutputSchema = strictOutputs.map {
      case (name, dt) => StructField(name, dt)
    }.toSeq

    val intermediateSchema = (outputs -- strictOutputs.keys).map {
      case (name, dt) => StructField(name, dt)
    }.toSeq

    (StructType(actualInputs).get,
      StructType(actualOutputs).get,
      StructType(intermediateSchema).get,
      StructType(strictOutputSchema).get)
  }
}
