package org.apache.spark.ml.bundle.extension.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.ml.mleap.classification.OneVsRestModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
class OneVsRestOp extends SimpleSparkOp[OneVsRestModel] {
  override val Model: OpModel[SparkBundleContext, OneVsRestModel] = new OpModel[SparkBundleContext, OneVsRestModel] {
    override val klazz: Class[OneVsRestModel] = classOf[OneVsRestModel]

    override def opName: String = Bundle.BuiltinOps.classification.one_vs_rest

    override def store(model: Model, obj: OneVsRestModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      var i = 0
      for(cModel <- obj.models) {
        val name = s"model$i"
        ModelSerializer(context.bundleContext(name)).write(cModel)
        i = i + 1
        name
      }

      model.withValue("num_classes", Value.long(obj.models.length)).
        withValue("num_features", Value.long(obj.models.head.numFeatures))
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

  override def sparkLoad(uid: String, shape: NodeShape, model: OneVsRestModel): OneVsRestModel = {
    val labelMetadata = NominalAttribute.defaultAttr.
      withName(shape.output("prediction").name).
      withNumValues(model.models.length).
      toMetadata
    new OneVsRestModel(uid = uid, models = model.models, labelMetadata = labelMetadata)
  }

  override def sparkInputs(obj: OneVsRestModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: OneVsRestModel): Seq[SimpleParamSpec] = {
    Seq("probability" -> obj.probabilityCol,
      "prediction" -> obj.predictionCol)
  }
}
