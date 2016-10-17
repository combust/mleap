package ml.combust.mleap.runtime.bundle.ops.feature

import ml.combust.bundle.dsl.{Bundle, Model, Node, Shape, Value}
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.mleap.core.feature.PolynomialExpansionModel
import ml.combust.mleap.runtime.transformer.feature.PolynomialExpansion

/**
  * Created by mikhail on 10/16/16.
  */
object PolynomialExpansionOp extends OpNode[PolynomialExpansion, PolynomialExpansionModel] {
  override val Model: OpModel[PolynomialExpansionModel] = new OpModel[PolynomialExpansionModel] {
    override def opName: String = Bundle.BuiltinOps.feature.polynomial_expansion

    override def store(context: BundleContext, model: Model, obj: PolynomialExpansionModel): Model = {
      model.withAttr("degree", Value.long(obj.degree))
    }

    override def load(context: BundleContext, model: Model): PolynomialExpansionModel = {
      PolynomialExpansionModel(degree = model.value("degree").getLong.toInt)
    }
  }
  override def name(node: PolynomialExpansion): String = node.uid

  override def model(node: PolynomialExpansion): PolynomialExpansionModel = node.model

  override def load(context: BundleContext, node: Node, model: PolynomialExpansionModel): PolynomialExpansion = {
    PolynomialExpansion(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: PolynomialExpansion): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
