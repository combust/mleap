package org.apache.spark.ml.bundle.ops

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.serializer.GraphSerializer
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.{PipelineModel, Transformer}

/**
  * Created by hollinwilkins on 8/21/16.
  */
class PipelineOp extends SimpleSparkOp[PipelineModel] {
  override val Model: OpModel[SparkBundleContext, PipelineModel] = new OpModel[SparkBundleContext, PipelineModel] {
    override val klazz: Class[PipelineModel] = classOf[PipelineModel]

    override def opName: String = Bundle.BuiltinOps.pipeline

    override def store(model: Model, obj: PipelineModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val nodes = GraphSerializer(context).write(obj.stages).get
      model.withValue("nodes", Value.stringList(nodes))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): PipelineModel = {
      val nodes = GraphSerializer(context).read(model.value("nodes").getStringList).
        map(_.map(_.asInstanceOf[Transformer])).get.toArray
      new PipelineModel(uid = "", stages = nodes)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: PipelineModel): PipelineModel = {
    new PipelineModel(uid = uid, stages = model.stages)
  }

  override def sparkInputs(obj: PipelineModel): Seq[ParamSpec] = Seq()

  override def sparkOutputs(obj: PipelineModel): Seq[SimpleParamSpec] = Seq()

  override def load(node: Node, model: PipelineModel)(implicit context: BundleContext[SparkBundleContext]): PipelineModel = {
    new PipelineModel(uid = node.name, stages = model.stages)
  }
}
