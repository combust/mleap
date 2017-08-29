package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/25/16.
  */
class MultiLayerPerceptronClassifierOp extends SimpleSparkOp[MultilayerPerceptronClassificationModel] {
  override val Model: OpModel[SparkBundleContext, MultilayerPerceptronClassificationModel] = new OpModel[SparkBundleContext, MultilayerPerceptronClassificationModel] {
    override def opName: String = Bundle.BuiltinOps.classification.multi_layer_perceptron_classifier

    override val klazz: Class[MultilayerPerceptronClassificationModel] = classOf[MultilayerPerceptronClassificationModel]

    override def store(model: Model, obj: MultilayerPerceptronClassificationModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("layers", Value.longList(obj.layers.map(_.toLong))).
        withValue("weights", Value.vector(obj.weights.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MultilayerPerceptronClassificationModel = {
      new MultilayerPerceptronClassificationModel(uid = "",
        layers = model.value("layers").getLongList.map(_.toInt).toArray,
        weights = Vectors.dense(model.value("weights").getTensor[Double].toArray))
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: MultilayerPerceptronClassificationModel): MultilayerPerceptronClassificationModel = {
    new MultilayerPerceptronClassificationModel(uid = uid,layers = model.layers, weights = model.weights)
  }

  override def sparkInputs(obj: MultilayerPerceptronClassificationModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: MultilayerPerceptronClassificationModel): Seq[SimpleParamSpec] = {
    Seq("prediction" -> obj.predictionCol)
  }
}

