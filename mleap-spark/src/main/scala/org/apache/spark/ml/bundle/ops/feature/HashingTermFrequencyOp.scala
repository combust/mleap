package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.HashingTF

/**
  * Created by hollinwilkins on 8/21/16.
  */
class HashingTermFrequencyOp extends OpNode[SparkBundleContext, HashingTF, HashingTF] {
  override val Model: OpModel[SparkBundleContext, HashingTF] = new OpModel[SparkBundleContext, HashingTF] {
    override val klazz: Class[HashingTF] = classOf[HashingTF]

    override def opName: String = Bundle.BuiltinOps.feature.hashing_term_frequency

    override def store(model: Model, obj: HashingTF)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("num_features", Value.long(obj.getNumFeatures)).
        withAttr("binary", Value.boolean(obj.getBinary))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): HashingTF = {
      new HashingTF(uid = "").setNumFeatures(model.value("num_features").getLong.toInt).
        setBinary(model.value("binary").getBoolean)
    }
  }

  override val klazz: Class[HashingTF] = classOf[HashingTF]

  override def name(node: HashingTF): String = node.uid

  override def model(node: HashingTF): HashingTF = node

  override def load(node: Node, model: HashingTF)
                   (implicit context: BundleContext[SparkBundleContext]): HashingTF = {
    new HashingTF(uid = node.name).setNumFeatures(model.getNumFeatures).
      setBinary(model.getBinary).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: HashingTF): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
