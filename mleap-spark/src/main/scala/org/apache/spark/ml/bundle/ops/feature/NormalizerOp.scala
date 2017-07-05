package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.Normalizer

/**
  * Created by hollinwilkins on 9/24/16.
  */
class NormalizerOp extends SimpleSparkOp[Normalizer] {
  override val Model: OpModel[SparkBundleContext, Normalizer] = new OpModel[SparkBundleContext, Normalizer] {
    override val klazz: Class[Normalizer] = classOf[Normalizer]

    override def opName: String = Bundle.BuiltinOps.feature.normalizer

    override def store(model: Model, obj: Normalizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("p_norm", Value.double(obj.getP))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Normalizer = {
      new Normalizer(uid = "").setP(model.value("p_norm").getDouble)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: Normalizer): Normalizer = {
    new Normalizer(uid = uid)
  }

  override def sparkInputs(obj: Normalizer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: Normalizer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
