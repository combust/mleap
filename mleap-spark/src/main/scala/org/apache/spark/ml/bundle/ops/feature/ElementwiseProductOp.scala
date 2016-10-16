package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/23/16.
  */
object ElementwiseProductOp extends OpNode[ElementwiseProduct, ElementwiseProduct] {
  override val Model: OpModel[ElementwiseProduct] = new OpModel[ElementwiseProduct] {
    override def opName: String = Bundle.BuiltinOps.feature.elementwise_product

    override def store(context: BundleContext, model: Model, obj: ElementwiseProduct): Model = {
      model.withAttr("scaling_vec", Value.doubleVector(obj.getScalingVec.toArray))
    }

    override def load(context: BundleContext, model: Model): ElementwiseProduct = {
      new ElementwiseProduct(uid = "").setScalingVec(Vectors.dense(model.value("scaling_vec").getDoubleVector.toArray))
    }
  }

  override def name(node: ElementwiseProduct): String = node.uid

  override def model(node: ElementwiseProduct): ElementwiseProduct = node


  override def load(context: BundleContext, node: Node, model: ElementwiseProduct): ElementwiseProduct = {
    new ElementwiseProduct(uid = node.name).copy(model.extractParamMap()).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: ElementwiseProduct): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
