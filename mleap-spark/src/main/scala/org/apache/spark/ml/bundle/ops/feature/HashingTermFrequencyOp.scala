package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.param.Param

/**
  * Created by hollinwilkins on 8/21/16.
  */
class HashingTermFrequencyOp extends SimpleSparkOp[HashingTF] {
  override val Model: OpModel[SparkBundleContext, HashingTF] = new OpModel[SparkBundleContext, HashingTF] {
    override val klazz: Class[HashingTF] = classOf[HashingTF]

    override def opName: String = Bundle.BuiltinOps.feature.hashing_term_frequency

    override def store(model: Model, obj: HashingTF)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("num_features", Value.long(obj.getNumFeatures)).
        withValue("binary", Value.boolean(obj.getBinary))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): HashingTF = {
      new HashingTF(uid = "").setNumFeatures(model.value("num_features").getLong.toInt).
        setBinary(model.value("binary").getBoolean)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: HashingTF): HashingTF = {
    new HashingTF(uid = uid)
  }

  override def sparkInputs(obj: HashingTF): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: HashingTF): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
