package ml.combust.bundle.test_ops

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.{BundleContext, GraphSerializer}
import ml.combust.bundle.dsl._
import ml.combust.bundle.dsl

/**
  * Created by hollinwilkins on 8/21/16.
  */
case class PipelineModel(stages: Seq[Transformer])
case class Pipeline(uid: String, model: PipelineModel) extends Transformer

object PipelineOp extends OpNode[Pipeline, PipelineModel] {
  override val Model: OpModel[PipelineModel] = new OpModel[PipelineModel] {
    override def opName: String = Bundle.BuiltinOps.pipeline

    override def store(context: BundleContext, model: Model, obj: PipelineModel): Model = {
      model.withAttr(Attribute("nodes", Value.stringList(GraphSerializer(context).write(obj.stages))))
    }

    override def load(context: BundleContext, model: Model): PipelineModel = {
      PipelineModel(GraphSerializer(context).read(model.value("nodes").getStringList).
        map(_.asInstanceOf[Transformer]))
    }
  }

  override def name(node: Pipeline): String = node.uid

  override def model(node: Pipeline): PipelineModel = node.model

  override def load(context: BundleContext, node: dsl.Node, model: PipelineModel): Pipeline = {
    Pipeline(node.name, model)
  }

  override def shape(node: Pipeline): Shape = Shape()
}
