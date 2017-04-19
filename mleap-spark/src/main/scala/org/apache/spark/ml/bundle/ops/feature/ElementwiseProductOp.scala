package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.mleap.TypeConverters.fieldType

/**
  * Created by mikhail on 9/23/16.
  */
class ElementwiseProductOp extends OpNode[SparkBundleContext, ElementwiseProduct, ElementwiseProduct] {
  override val Model: OpModel[SparkBundleContext, ElementwiseProduct] = new OpModel[SparkBundleContext, ElementwiseProduct] {
    override val klazz: Class[ElementwiseProduct] = classOf[ElementwiseProduct]

    override def opName: String = Bundle.BuiltinOps.feature.elementwise_product

    override def store(model: Model, obj: ElementwiseProduct)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("scaling_vec", Value.vector(obj.getScalingVec.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): ElementwiseProduct = {
      new ElementwiseProduct(uid = "").setScalingVec(Vectors.dense(model.value("scaling_vec").getTensor[Double].toArray))
    }
  }

  override val klazz: Class[ElementwiseProduct] = classOf[ElementwiseProduct]

  override def name(node: ElementwiseProduct): String = node.uid

  override def model(node: ElementwiseProduct): ElementwiseProduct = node


  override def load(node: Node, model: ElementwiseProduct)
                   (implicit context: BundleContext[SparkBundleContext]): ElementwiseProduct = {
    new ElementwiseProduct(uid = node.name).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name).
      setScalingVec(model.getScalingVec)
  }

  override def shape(node: ElementwiseProduct)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val dataset = context.context.dataset
    Shape().withStandardIO(node.getInputCol, fieldType(node.getInputCol, dataset),
      node.getOutputCol, fieldType(node.getOutputCol, dataset))
  }
}
