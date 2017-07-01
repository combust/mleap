package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.bundle.dsl._
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.classification.{ClassificationModel, OneVsRestModel}

/**
  * Created by hollinwilkins on 8/21/16.
  */
class OneVsRestOp extends OpNode[SparkBundleContext, OneVsRestModel, OneVsRestModel] {
  override val Model: OpModel[SparkBundleContext, OneVsRestModel] = new OpModel[SparkBundleContext, OneVsRestModel] {
    override val klazz: Class[OneVsRestModel] = classOf[OneVsRestModel]

    override def opName: String = Bundle.BuiltinOps.classification.one_vs_rest

    override def store(model: Model, obj: OneVsRestModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      var i = 0
      for(cModel <- obj.models) {
        val name = s"model$i"
        ModelSerializer(context.bundleContext(name)).write(cModel).get
        i = i + 1
        name
      }

      model.withAttr("num_classes", Value.long(obj.models.length))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): OneVsRestModel = {
      val numClasses = model.value("num_classes").getLong.toInt

      val models = (0 until numClasses).toArray.map {
        i => ModelSerializer(context.bundleContext(s"model$i")).read().get.asInstanceOf[ClassificationModel[_, _]]
      }

      val labelMetadata = NominalAttribute.defaultAttr.
        withName("prediction").
        withNumValues(models.length).
        toMetadata
      new OneVsRestModel(uid = "", models = models, labelMetadata = labelMetadata)
    }
  }

  override val klazz: Class[OneVsRestModel] = classOf[OneVsRestModel]

  override def name(node: OneVsRestModel): String = node.uid

  override def model(node: OneVsRestModel): OneVsRestModel = node

  override def load(node: Node, model: OneVsRestModel)
                   (implicit context: BundleContext[SparkBundleContext]): OneVsRestModel = {
    assert(node.shape.getOutput("probability").isEmpty, "default OneVsRestModel does not support probability columns, use mleap-spark-extension library")

    val labelMetadata = NominalAttribute.defaultAttr.
      withName(node.shape.output("prediction").name).
      withNumValues(model.models.length).
      toMetadata
    val m = new OneVsRestModel(uid = node.name, models = model.models, labelMetadata = labelMetadata)
    m.set(m.featuresCol, node.shape.input("features").name)
    m.set(m.predictionCol, node.shape.output("prediction").name)
    m
  }

  override def shape(node: OneVsRestModel): NodeShape = NodeShape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
