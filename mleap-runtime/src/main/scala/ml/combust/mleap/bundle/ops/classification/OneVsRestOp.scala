package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.classification.{OneVsRestModel, ProbabilisticClassificationModel}
import ml.combust.mleap.runtime.transformer.classification.OneVsRest
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
class OneVsRestOp extends OpNode[MleapContext, OneVsRest, OneVsRestModel] {
  override val Model: OpModel[MleapContext, OneVsRestModel] = new OpModel[MleapContext, OneVsRestModel] {
    override val klazz: Class[OneVsRestModel] = classOf[OneVsRestModel]

    override def opName: String = Bundle.BuiltinOps.classification.one_vs_rest

    override def store(model: Model, obj: OneVsRestModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      var i = 0
      for(cModel <- obj.classifiers) {
        val name = s"model$i"
        ModelSerializer(context.bundleContext(name)).write(cModel)
        i = i + 1
        name
      }

      model.withAttr("num_classes", Value.long(obj.classifiers.length))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): OneVsRestModel = {
      val numClasses = model.value("num_classes").getLong.toInt

      val models = (0 until numClasses).toArray.map {
        i => ModelSerializer(context.bundleContext(s"model$i")).read().asInstanceOf[ProbabilisticClassificationModel]
      }

      OneVsRestModel(classifiers = models)
    }
  }

  override val klazz: Class[OneVsRest] = classOf[OneVsRest]

  override def name(node: OneVsRest): String = node.uid

  override def model(node: OneVsRest): OneVsRestModel = node.model

  override def load(node: Node, model: OneVsRestModel)
                   (implicit context: BundleContext[MleapContext]): OneVsRest = {
    OneVsRest(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      probabilityCol = node.shape.getOutput("probability").map(_.name),
      model = model)
  }

  override def shape(node: OneVsRest)(implicit context: BundleContext[MleapContext]): Shape = {
    Shape().withInput(node.featuresCol, "features").
      withOutput(node.predictionCol, "prediction").
      withOutput(node.probabilityCol, "probability"  )
  }
}
