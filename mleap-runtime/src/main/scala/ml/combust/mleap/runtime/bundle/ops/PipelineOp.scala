package ml.combust.mleap.runtime.bundle.ops

import ml.combust.mleap.runtime.transformer.{Pipeline, Transformer}
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.{BundleContext, GraphSerializer}
import ml.combust.bundle.dsl._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object PipelineOp extends OpNode[Pipeline, Pipeline] {
  override val Model: OpModel[Pipeline] = new OpModel[Pipeline] {
    override def opName: String = Bundle.BuiltinOps.pipeline

    override def store(context: BundleContext, model: Model, obj: Pipeline): Model = {
      val nodes = GraphSerializer(context).write(obj.transformers)
      model.withAttr("nodes", Value.stringList(nodes))
    }

    override def load(context: BundleContext, model: Model): Pipeline = {
      val nodes = GraphSerializer(context).read(model.value("nodes").getStringList).map(_.asInstanceOf[Transformer])
      Pipeline(transformers = nodes)
    }
  }

  override def name(node: Pipeline): String = node.uid

  override def model(node: Pipeline): Pipeline = node

  override def load(context: BundleContext, node: Node, model: Pipeline): Pipeline = {
    model.copy(uid = node.name)
  }

  override def shape(node: Pipeline): Shape = Shape()
}
