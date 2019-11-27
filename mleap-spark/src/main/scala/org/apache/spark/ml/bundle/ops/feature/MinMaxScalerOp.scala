package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.MinMaxScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
class MinMaxScalerOp extends SimpleSparkOp[MinMaxScalerModel] {
  override val Model: OpModel[SparkBundleContext, MinMaxScalerModel] = new OpModel[SparkBundleContext, MinMaxScalerModel] {
    override val klazz: Class[MinMaxScalerModel] = classOf[MinMaxScalerModel]

    override def opName: String = Bundle.BuiltinOps.feature.min_max_scaler

    override def store(model: Model, obj: MinMaxScalerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      require(obj.getMin == 0.0, "MLeap only supports min set to 0.0")
      require(obj.getMax == 1.0, "MLeap only supports max set to 1.0")
      model.withValue("min", Value.vector(obj.originalMin.toArray)).
        withValue("max", Value.vector(obj.originalMax.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MinMaxScalerModel = {
      new MinMaxScalerModel(uid = "",
        originalMin = Vectors.dense(model.value("min").getTensor[Double].toArray),
        originalMax = Vectors.dense(model.value("max").getTensor[Double].toArray))
        .setMin(0.0)
        .setMax(1.0)
    }

  }

  override def sparkLoad(uid: String, shape: NodeShape, model: MinMaxScalerModel): MinMaxScalerModel = {
    val m = new MinMaxScalerModel(uid = uid,
      originalMin = model.originalMin,
      originalMax = model.originalMax)
    if (model.isDefined(model.max)) { m.setMax(model.getMax)}
    if (model.isDefined(model.min)) { m.setMin(model.getMin)}
    m
  }

  override def sparkInputs(obj: MinMaxScalerModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: MinMaxScalerModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
