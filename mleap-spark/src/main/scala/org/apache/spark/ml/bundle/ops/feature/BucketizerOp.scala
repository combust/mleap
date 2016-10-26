package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.Bucketizer

/**
  * Created by mikhail on 9/22/16.
  */
class BucketizerOp extends OpNode[SparkBundleContext, Bucketizer, Bucketizer] {
  override val Model: OpModel[SparkBundleContext, Bucketizer] = new OpModel[SparkBundleContext, Bucketizer] {
    override val klazz: Class[Bucketizer] = classOf[Bucketizer]

    override def opName: String = Bundle.BuiltinOps.feature.bucketizer

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: Bucketizer): Model = {
      model.withAttr("splits", Value.doubleList(obj.getSplits))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): Bucketizer = {
      new Bucketizer(uid = "").setSplits(model.value("splits").getDoubleList.toArray)
    }
  }

  override val klazz: Class[Bucketizer] = classOf[Bucketizer]

  override def name(node: Bucketizer): String = node.uid

  override def model(node: Bucketizer): Bucketizer = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: Bucketizer): Bucketizer = {
    new Bucketizer(uid = node.name).copy(model.extractParamMap()).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: Bucketizer): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
