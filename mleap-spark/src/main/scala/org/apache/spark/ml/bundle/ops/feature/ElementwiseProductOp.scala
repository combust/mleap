package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.dsl._
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import org.apache.spark.ml.feature.ElementwiseProduct

/**
  * Created by mikhail on 9/23/16.
  */
object ElementwiseProductOp extends OpNode[ElementwiseProduct, ElementwiseProduct] {
  override val Model: OpModel[ElementwiseProduct] = new OpModel[ElementwiseProduct] {
    override def opName: String = Bundle.BuiltinOps.feature.elementwise_product

    override def store(context: BundleContext, model: WritableModel, obj: ElementwiseProduct): WritableModel = {
      model.withAttr(Attribute("scalingVec", Value.doubleVector(obj.getScalingVec.toArray)))
    }

    override def load(context: BundleContext, model: ReadableModel): ElementwiseProduct = {
      new ElementwiseProduct(uid = "").setScalingVec(model.value("scalingVec").getDoubleVector.toArray)
    }

  }

  override def name(node: ElementwiseProduct): String = node.uid

  override def model(node: ElementwiseProduct): ElementwiseProduct = node


  override def load(context: BundleContext, node: ReadableNode, model: ElementwiseProduct): ElementwiseProduct = {
    new ElementwiseProduct(uid = node.name).copy(model.extractParamMap()).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: ElementwiseProduct): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
