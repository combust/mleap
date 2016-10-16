package org.apache.spark.ml.bundle.ops

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.{BundleContext, GraphSerializer}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.{PipelineModel, Transformer}

/**
  * Created by hollinwilkins on 8/21/16.
  */
object PipelineOp extends OpNode[PipelineModel, PipelineModel] {
  override val Model: OpModel[PipelineModel] = new OpModel[PipelineModel] {
    override def opName: String = Bundle.BuiltinOps.pipeline

    override def store(context: BundleContext, model: Model, obj: PipelineModel): Model = {
      val nodes = GraphSerializer(context).write(obj.stages)
      model.withAttr("nodes", Value.stringList(nodes))
    }

    override def load(context: BundleContext, model: Model): PipelineModel = {
      val nodes = GraphSerializer(context).read(model.value("nodes").getStringList).map(_.asInstanceOf[Transformer]).toArray
      new PipelineModel(uid = "", stages = nodes)
    }
  }

  override def name(node: PipelineModel): String = node.uid

  override def model(node: PipelineModel): PipelineModel = node

  override def load(context: BundleContext, node: Node, model: PipelineModel): PipelineModel = {
    new PipelineModel(uid = node.name, stages = model.stages)
  }

  override def shape(node: PipelineModel): Shape = Shape()

  override def children(node: PipelineModel): Option[Array[Any]] = Some(node.stages.toArray)
}
