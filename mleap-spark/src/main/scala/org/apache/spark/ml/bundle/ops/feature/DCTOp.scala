package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.DCT

/**
  * Created by hollinwilkins on 12/28/16.
  */
class DCTOp extends OpNode[SparkBundleContext, DCT, DCT] {
  override val Model: OpModel[SparkBundleContext, DCT] = new OpModel[SparkBundleContext, DCT] {
    override val klazz: Class[DCT] = classOf[DCT]

    override def opName: String = Bundle.BuiltinOps.feature.dct

    override def store(model: Model, obj: DCT)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("inverse", Value.boolean(obj.getInverse))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): DCT = {
      new DCT(uid = "").setInverse(model.value("inverse").getBoolean)
    }
  }

  override val klazz: Class[DCT] = classOf[DCT]

  override def name(node: DCT): String = node.uid

  override def model(node: DCT): DCT = node

  override def load(node: Node, model: DCT)
                   (implicit context: BundleContext[SparkBundleContext]): DCT = {
    new DCT(uid = node.name).setInverse(model.getInverse).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: DCT): NodeShape = NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
