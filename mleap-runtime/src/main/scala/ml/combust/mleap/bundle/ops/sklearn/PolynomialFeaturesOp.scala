package ml.combust.mleap.bundle.ops.sklearn

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.sklearn.PolynomialFeaturesModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.sklearn.PolynomialFeatures

class PolynomialFeaturesOp extends MleapOp[PolynomialFeatures, PolynomialFeaturesModel] {
  override val Model: OpModel[MleapContext, PolynomialFeaturesModel] = new OpModel[MleapContext, PolynomialFeaturesModel] {

    override val klazz: Class[PolynomialFeaturesModel] = classOf[PolynomialFeaturesModel]

    override def opName: String = "sklearn_polynomial_expansion"

    override def store(model: Model, obj: PolynomialFeaturesModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("combinations", Value.string(obj.combinations))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): PolynomialFeaturesModel = {
      PolynomialFeaturesModel(model.value("combinations").getString)
    }
  }

  override def model(node: PolynomialFeatures): PolynomialFeaturesModel = node.model
}
