package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.PolynomialExpansion

/**
  * Created by mikhail on 10/16/16.
  */
class PolynomialExpansionOp extends OpNode[SparkBundleContext, PolynomialExpansion, PolynomialExpansion] {
  override val Model: OpModel[SparkBundleContext, PolynomialExpansion] = new OpModel[SparkBundleContext, PolynomialExpansion] {
    override val klazz: Class[PolynomialExpansion] = classOf[PolynomialExpansion]

    override def opName: String = Bundle.BuiltinOps.feature.polynomial_expansion

    override def store(model: Model, obj: PolynomialExpansion)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("degree", Value.long(obj.getDegree))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): PolynomialExpansion = {
      new PolynomialExpansion(uid = "").setDegree(model.value("degree").getLong.toInt)
    }

  }

  override val klazz: Class[PolynomialExpansion] = classOf[PolynomialExpansion]

  override def name(node: PolynomialExpansion): String = node.uid

  override def model(node: PolynomialExpansion): PolynomialExpansion = node


  override def load(node: Node, model: PolynomialExpansion)
                   (implicit context: BundleContext[SparkBundleContext]): PolynomialExpansion = {
    new PolynomialExpansion(uid = node.name).
      setDegree(model.getDegree).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: PolynomialExpansion): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
