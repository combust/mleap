package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.PolynomialExpansionModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.PolynomialExpansion

/**
  * Created by mikhail on 10/16/16.
  */
class PolynomialExpansionOp extends OpNode[MleapContext, PolynomialExpansion, PolynomialExpansionModel] {
  override val Model: OpModel[MleapContext, PolynomialExpansionModel] = new OpModel[MleapContext, PolynomialExpansionModel] {
    override val klazz: Class[PolynomialExpansionModel] = classOf[PolynomialExpansionModel]

    override def opName: String = Bundle.BuiltinOps.feature.polynomial_expansion

    override def store(model: Model, obj: PolynomialExpansionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("degree", Value.long(obj.degree))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): PolynomialExpansionModel = {
      PolynomialExpansionModel(degree = model.value("degree").getLong.toInt)
    }
  }

  override val klazz: Class[PolynomialExpansion] = classOf[PolynomialExpansion]

  override def name(node: PolynomialExpansion): String = node.uid

  override def model(node: PolynomialExpansion): PolynomialExpansionModel = node.model

  override def load(node: Node, model: PolynomialExpansionModel)
                   (implicit context: BundleContext[MleapContext]): PolynomialExpansion = {
    PolynomialExpansion(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: PolynomialExpansion): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
