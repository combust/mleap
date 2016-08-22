package ml.combust.mleap.runtime.serialization.bundle.ops.classification

import ml.combust.mleap.core.classification.{BinaryClassificationModel, OneVsRestModel}
import ml.combust.mleap.runtime.transformer.classification.OneVsRest
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, ModelSerializer}
import ml.bundle.dsl._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object OneVsRestOp extends OpNode[OneVsRest, OneVsRestModel] {
  override val Model: OpModel[OneVsRestModel] = new OpModel[OneVsRestModel] {
    override def opName: String = Bundle.BuiltinOps.classification.one_vs_rest

    override def store(context: BundleContext, model: WritableModel, obj: OneVsRestModel): WritableModel = {
      var i = 0
      for(cModel <- obj.classifiers) {
        val name = s"model$i"
        ModelSerializer(context.bundleContext(name)).write(cModel)
        i = i + 1
        name
      }

      model.withAttr(Attribute("num_classes", Value.long(obj.classifiers.length)))
    }

    override def load(context: BundleContext, model: ReadableModel): OneVsRestModel = {
      val numClasses = model.value("num_classes").getLong.toInt

      val models = (0 until numClasses).toArray.map {
        i => ModelSerializer(context.bundleContext(s"model$i")).read().asInstanceOf[BinaryClassificationModel]
      }

      OneVsRestModel(classifiers = models)
    }
  }

  override def name(node: OneVsRest): String = node.uid

  override def model(node: OneVsRest): OneVsRestModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: OneVsRestModel): OneVsRest = {
    OneVsRest(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      probabilityCol = node.shape.getOutput("probability").map(_.name),
      model = model)
  }

  override def shape(node: OneVsRest): Shape = {
    val s = Shape().withInput(node.featuresCol, "features").
      withOutput(node.predictionCol, "prediction")
    node.probabilityCol.map {
      pc => s.withOutput(pc, "probability")
    }.getOrElse(s)
  }
}
