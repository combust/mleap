package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.PolynomialExpansionModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.PolynomialExpansion

/**
  * Created by mikhail on 10/16/16.
  */
class PolynomialExpansionOp extends MleapOp[PolynomialExpansion, PolynomialExpansionModel] {
  override val Model: OpModel[MleapContext, PolynomialExpansionModel] = new OpModel[MleapContext, PolynomialExpansionModel] {
    override val klazz: Class[PolynomialExpansionModel] = classOf[PolynomialExpansionModel]

    override def opName: String = Bundle.BuiltinOps.feature.polynomial_expansion

    override def store(model: Model, obj: PolynomialExpansionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("degree", Value.long(obj.degree))
      .withValue("input_size", Value.long(obj.inputSize))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): PolynomialExpansionModel = {
      PolynomialExpansionModel(degree = model.value("degree").getLong.toInt,
        inputSize = model.value("input_size").getLong.toInt)
    }
  }

  override def model(node: PolynomialExpansion): PolynomialExpansionModel = node.model
}
