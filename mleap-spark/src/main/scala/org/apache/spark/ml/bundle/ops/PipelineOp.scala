package org.apache.spark.ml.bundle.ops

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.GraphSerializer
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.{PipelineModel, Transformer}

/**
  * Created by hollinwilkins on 8/21/16.
  */
class PipelineOp extends OpNode[SparkBundleContext, PipelineModel, PipelineModel] {
  override val Model: OpModel[SparkBundleContext, PipelineModel] = new OpModel[SparkBundleContext, PipelineModel] {
    override val klazz: Class[PipelineModel] = classOf[PipelineModel]

    override def opName: String = Bundle.BuiltinOps.pipeline

    override def store(model: Model, obj: PipelineModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val nodes = GraphSerializer(context).write(obj.stages)
      model.withAttr("nodes", Value.stringList(nodes))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): PipelineModel = {
      val nodes = GraphSerializer(context).read(model.value("nodes").getStringList).map(_.asInstanceOf[Transformer]).toArray
      new PipelineModel(uid = "", stages = nodes)
    }
  }

  override val klazz: Class[PipelineModel] = classOf[PipelineModel]

  override def name(node: PipelineModel): String = node.uid

  override def model(node: PipelineModel): PipelineModel = node

  override def load(node: Node, model: PipelineModel)
                   (implicit context: BundleContext[SparkBundleContext]): PipelineModel = {
    new PipelineModel(uid = node.name, stages = model.stages)
  }

  override def shape(node: PipelineModel): Shape = Shape()

  override def children(node: PipelineModel): Option[Array[Any]] = Some(node.stages.toArray)
}
