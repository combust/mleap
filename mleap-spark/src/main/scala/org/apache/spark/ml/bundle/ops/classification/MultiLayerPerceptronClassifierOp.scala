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
      val thresholds = if(obj.isSet(obj.thresholds)) {
        Some(obj.getThresholds)
      } else None
      model.
        withValue("weights", Value.vector(obj.weights.toArray)).
        withValue("thresholds", thresholds.map(_.toSeq).map(Value.doubleList))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MultilayerPerceptronClassificationModel = {
      val m = new MultilayerPerceptronClassificationModel(uid = "",
        weights = Vectors.dense(model.value("weights").getTensor[Double].toArray))
      model.getValue("thresholds").
        map(t => m.setThresholds(t.getDoubleList.toArray)).
        getOrElse(m)
    }

  }

  override def sparkLoad(uid: String, shape: NodeShape, model: MultilayerPerceptronClassificationModel): MultilayerPerceptronClassificationModel = {
    val m = new MultilayerPerceptronClassificationModel(uid = uid,
      weights = model.weights)
    if (model.isSet(model.thresholds)) m.setThresholds(model.getThresholds)
    m
  }

  override def sparkInputs(obj: MultilayerPerceptronClassificationModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: MultilayerPerceptronClassificationModel):  Seq[SimpleParamSpec] = {
    Seq("raw_prediction" -> obj.rawPredictionCol,
      "probability" -> obj.probabilityCol,
      "prediction" -> obj.predictionCol)
  }
}

