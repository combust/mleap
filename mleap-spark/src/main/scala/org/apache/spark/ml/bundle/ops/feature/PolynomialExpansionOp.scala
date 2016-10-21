package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import org.apache.spark.ml.feature.PolynomialExpansion

/**
  * Created by mikhail on 10/16/16.
  */
object PolynomialExpansionOp extends OpNode[PolynomialExpansion, PolynomialExpansion] {
  override val Model: OpModel[PolynomialExpansion] = new OpModel[PolynomialExpansion] {
    override def opName: String = Bundle.BuiltinOps.feature.polynomial_expansion

    override def store(context: BundleContext, model: Model, obj: PolynomialExpansion): Model = {
      model.withAttr("degree", Value.long(obj.getDegree))
    }

    override def load(context: BundleContext, model: Model): PolynomialExpansion = {
      new PolynomialExpansion(uid = "").setDegree(model.value("degree").getLong.toInt)
    }

  }
  override def name(node: PolynomialExpansion): String = node.uid

  override def model(node: PolynomialExpansion): PolynomialExpansion = node


  override def load(context: BundleContext, node: Node, model: PolynomialExpansion): PolynomialExpansion = {
    new PolynomialExpansion().
      setDegree(model.getDegree).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: PolynomialExpansion): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
