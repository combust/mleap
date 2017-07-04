package ml.combust.mleap.bundle.ops

import ml.combust.bundle.BundleContext
import ml.combust.mleap.runtime.transformer.{Pipeline, Transformer}
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.serializer.GraphSerializer
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
class PipelineOp extends MleapOp[Pipeline, Pipeline] {
  override val Model: OpModel[MleapContext, Pipeline] = new OpModel[MleapContext, Pipeline] {
    override val klazz: Class[Pipeline] = classOf[Pipeline]

    override def opName: String = Bundle.BuiltinOps.pipeline

    override def store(model: Model, obj: Pipeline)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val nodes = GraphSerializer(context).write(obj.transformers).get
      model.withValue("nodes", Value.stringList(nodes))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): Pipeline = {
      val nodes = GraphSerializer(context).read(model.value("nodes").getStringList).
        map(_.map(_.asInstanceOf[Transformer])).get
      Pipeline(transformers = nodes)
    }
  }

  override def model(node: Pipeline): Pipeline = node
}
