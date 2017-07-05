package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.runtime.transformer.feature.BucketizerUtil._
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.param.Param

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
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Bucketizer = {
      new Bucketizer(uid = "").setSplits(restoreSplits(model.value("splits").getDoubleList.toArray))
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: Bucketizer): Bucketizer = {
    new Bucketizer(uid = uid)
  }

  override def sparkInputs(obj: Bucketizer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: Bucketizer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
