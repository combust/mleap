package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.regression.GeneralizedLinearRegressionModel
import ml.combust.mleap.core.regression.GeneralizedLinearRegressionModel.{Family, FamilyAndLink, Link}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.regression.GeneralizedLinearRegression
import ml.combust.mleap.runtime.types.BundleTypeConverters.{bundleToMleapShape, mleapToBundleShape}
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/28/16.
  */
class GeneralizedLinearRegressionOp extends MleapOp[GeneralizedLinearRegression, GeneralizedLinearRegressionModel] {
  override val Model: OpModel[MleapContext, GeneralizedLinearRegressionModel] = new OpModel[MleapContext, GeneralizedLinearRegressionModel] {
    override val klazz: Class[GeneralizedLinearRegressionModel] = classOf[GeneralizedLinearRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.generalized_linear_regression

    override def store(model: Model, obj: GeneralizedLinearRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("coefficients", Value.vector(obj.coefficients.toArray)).
        withValue("intercept", Value.double(obj.intercept)).
        withValue("family", Value.string(obj.fal.family.name)).
        withValue("link", Value.string(obj.fal.link.name)).
        withValue("output_shapes", Value.dataShapeList(obj.outputShapes.map(mleapToBundleShape)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): GeneralizedLinearRegressionModel = {

      val outputShapes = model.value("output_shapes").getDataShapeList.map(bundleToMleapShape)

      GeneralizedLinearRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble,
        fal = new FamilyAndLink(Family.fromName(model.value("family").getString),
          Link.fromName(model.value("link").getString)), outputShapes)
    }
  }

  override def model(node: GeneralizedLinearRegression): GeneralizedLinearRegressionModel = node.model
}
