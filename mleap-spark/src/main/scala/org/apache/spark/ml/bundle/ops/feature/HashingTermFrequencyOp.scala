package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.HashingTF

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
        withValue("binary", Value.boolean(obj.getBinary)).
        withValue("version", Value.long(1))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): HashingTF = {
      val version = model.getValue("version").map(_.getLong.toInt).getOrElse(1)
      val numFeatures = model.value("num_features").getLong.toInt
      val binary = model.value("binary").getBoolean
      new HashingTF(uid = "").setNumFeatures(numFeatures).setBinary(binary)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: HashingTF): HashingTF = {
    new HashingTF(uid = uid).setNumFeatures(model.getNumFeatures).setBinary(model.getBinary)
  }

  override def sparkInputs(obj: HashingTF): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: HashingTF): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
