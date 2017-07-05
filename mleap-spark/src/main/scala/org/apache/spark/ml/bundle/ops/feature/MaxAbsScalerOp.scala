package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.MaxAbsScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
class MaxAbsScalerOp extends SimpleSparkOp[MaxAbsScalerModel]{
  override val Model: OpModel[SparkBundleContext, MaxAbsScalerModel] = new OpModel[SparkBundleContext, MaxAbsScalerModel] {
    override val klazz: Class[MaxAbsScalerModel] = classOf[MaxAbsScalerModel]

    override def opName: String = Bundle.BuiltinOps.feature.max_abs_scaler

    override def store(model: Model, obj: MaxAbsScalerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("maxAbs", Value.vector(obj.maxAbs.toArray))
  }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MaxAbsScalerModel = {
      new MaxAbsScalerModel(uid = "",
        maxAbs = Vectors.dense(model.value("maxAbs").getTensor[Double].toArray))
    }

  }

  override def sparkLoad(uid: String, shape: NodeShape, model: MaxAbsScalerModel): MaxAbsScalerModel = {
    new MaxAbsScalerModel(uid = uid,
      maxAbs = model.maxAbs)
  }

  override def sparkInputs(obj: MaxAbsScalerModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: MaxAbsScalerModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
