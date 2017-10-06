package ml.combust.mleap.bundle.ops

import ml.combust.bundle.BundleContext
import ml.combust.mleap.runtime.transformer.{Pipeline, PipelineModel}
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.serializer.GraphSerializer
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.frame.{MleapContext, Transformer}

/**
  * Created by hollinwilkins on 8/22/16.
  */
class PipelineOp extends MleapOp[Pipeline, PipelineModel] {
  override val Model: OpModel[MleapContext, PipelineModel] = new OpModel[MleapContext, PipelineModel] {
    override val klazz: Class[PipelineModel] = classOf[PipelineModel]

    override def opName: String = Bundle.BuiltinOps.pipeline

    override def store(model: Model, obj: PipelineModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val nodes = GraphSerializer(context).write(obj.transformers).get
      model.withValue("nodes", Value.stringList(nodes))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): PipelineModel = {
      val nodes = GraphSerializer(context).read(model.value("nodes").getStringList).
        map(_.map(_.asInstanceOf[Transformer])).get
      PipelineModel(transformers = nodes)
    }
  }

  override def model(node: Pipeline): PipelineModel = node.model
}
