package ml.combust.mleap.bundle.ops.ensemble

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.runtime.transformer.ensemble.{CategoricalDrilldown, CategoricalDrilldownModel}

class CategoricalDrilldownOp extends MleapOp[CategoricalDrilldown, CategoricalDrilldownModel] {
  override val Model: OpModel[MleapContext, CategoricalDrilldownModel] = new OpModel[MleapContext, CategoricalDrilldownModel] {
    override val klazz: Class[CategoricalDrilldownModel] = classOf[CategoricalDrilldownModel]

    override def opName: String = Bundle.BuiltinOps.ensemble.categorical_drilldown

    override def store(model: Model, obj: CategoricalDrilldownModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val indexedTransformers = obj.transformers.toSeq.zipWithIndex
      val labels = indexedTransformers.map(_._1._1)

      for(((_, transformer), i) <- indexedTransformers) {
        val name = s"model$i"
        ModelSerializer(context.bundleContext(name)).write(transformer).get
        name
      }

      model.withValue("labels", Value.stringList(labels))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): CategoricalDrilldownModel = {
      val labels = model.value("labels").getStringList
      val transformers = labels.indices.map {
        i => ModelSerializer(context.bundleContext(s"model$i")).read().get.asInstanceOf[Transformer]
      }
      val lookup = labels.zip(transformers).toMap

      CategoricalDrilldownModel(lookup)
    }
  }

  override def model(node: CategoricalDrilldown): CategoricalDrilldownModel = node.model
}
