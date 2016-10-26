package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.HashingTF

/**
  * Created by hollinwilkins on 8/21/16.
  */
object HashingTermFrequencyOp extends OpNode[SparkBundleContext, HashingTF, HashingTF] {
  override val Model: OpModel[SparkBundleContext, HashingTF] = new OpModel[SparkBundleContext, HashingTF] {
    override def opName: String = Bundle.BuiltinOps.feature.hashing_term_frequency

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: HashingTF): Model = {
      model.withAttr("num_features", Value.long(obj.getNumFeatures)).
        withAttr("binary", Value.boolean(obj.getBinary))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): HashingTF = {
      new HashingTF(uid = "").setNumFeatures(model.value("num_features").getLong.toInt).
        setBinary(model.value("binary").getBoolean)
    }
  }

  override def name(node: HashingTF): String = node.uid

  override def model(node: HashingTF): HashingTF = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: HashingTF): HashingTF = {
    new HashingTF(uid = node.name).setNumFeatures(model.getNumFeatures).
      setBinary(model.getBinary).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: HashingTF): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
