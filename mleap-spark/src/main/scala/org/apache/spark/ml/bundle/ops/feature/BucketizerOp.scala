package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.core.feature.HandleInvalid
import ml.combust.mleap.runtime.transformer.feature.BucketizerUtil._
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.Bucketizer

/**
  * Created by mikhail on 9/22/16.
  */
class BucketizerOp extends SimpleSparkOp[Bucketizer] {
  override val Model: OpModel[SparkBundleContext, Bucketizer] = new OpModel[SparkBundleContext, Bucketizer] {
    override val klazz: Class[Bucketizer] = classOf[Bucketizer]

    override def opName: String = Bundle.BuiltinOps.feature.bucketizer

    override def store(model: Model, obj: Bucketizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("splits", Value.doubleList(obj.getSplits))
        .withValue("handle_invalid", Value.string(obj.getHandleInvalid))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Bucketizer = {
      val m = new Bucketizer(uid = "").setSplits(restoreSplits(model.value("splits").getDoubleList.toArray))
      val handleInvalid = model.getValue("handle_invalid").map(_.getString).getOrElse(HandleInvalid.default.asParamString)

      m.set(m.handleInvalid, handleInvalid)
      m
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: Bucketizer): Bucketizer = {
    val m = new Bucketizer(uid = uid).setSplits(model.getSplits)
    m.set(m.handleInvalid, model.getHandleInvalid)
    m
  }

  override def sparkInputs(obj: Bucketizer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: Bucketizer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
