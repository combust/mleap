package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.dsl._
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import org.apache.spark.ml.feature.Bucketizer

/**
  * Created by mikhail on 9/22/16.
  */
object BucketizerOp extends OpNode[Bucketizer, Bucketizer] {
  override val Model: OpModel[Bucketizer] = new OpModel[Bucketizer] {
    override def opName: String = Bundle.BuiltinOps.feature.bucketizer

    override def store(context: BundleContext, model: WritableModel, obj: Bucketizer): WritableModel = {
      model.withAttr(Attribute("splits", Value.doubleList(obj.getSplits)))
    }

    override def load(context: BundleContext, model: ReadableModel): Bucketizer = {
      new Bucketizer(uid = "").setSplits(model.value("splits").getDoubleList.toArray)
    }
  }

  override def name(node: Bucketizer): String = node.uid

  override def model(node: Bucketizer): Bucketizer = node

  override def load(context: BundleContext, node: ReadableNode, model: Bucketizer): Bucketizer = {
    new Bucketizer(uid = node.name).copy(model.extractParamMap()).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: Bucketizer): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
