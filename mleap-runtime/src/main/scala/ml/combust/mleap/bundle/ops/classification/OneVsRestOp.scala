package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.classification.{ClassificationModel, OneVsRestModel}
import ml.combust.mleap.runtime.transformer.classification.OneVsRest
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.bundle.dsl._
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
class OneVsRestOp extends MleapOp[OneVsRest, OneVsRestModel] {
  override val Model: OpModel[MleapContext, OneVsRestModel] = new OpModel[MleapContext, OneVsRestModel] {
    override val klazz: Class[OneVsRestModel] = classOf[OneVsRestModel]

    override def opName: String = Bundle.BuiltinOps.classification.one_vs_rest

    override def store(model: Model, obj: OneVsRestModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      var i = 0
      for(cModel <- obj.classifiers) {
        val name = s"model$i"
        ModelSerializer(context.bundleContext(name)).write(cModel).get
        i = i + 1
        name
      }

      model.withValue("num_classes", Value.long(obj.classifiers.length)).
        withValue("num_features", Value.long(obj.numFeatures))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): OneVsRestModel = {
      val numClasses = model.value("num_classes").getLong.toInt
      val numFeatures = model.value("num_features").getLong.toInt

      val models = (0 until numClasses).toArray.map {
        i => ModelSerializer(context.bundleContext(s"model$i")).read().get.asInstanceOf[ClassificationModel]
      }

      OneVsRestModel(classifiers = models, numFeatures = numFeatures)
    }
  }

  override def model(node: OneVsRest): OneVsRestModel = node.model
}
