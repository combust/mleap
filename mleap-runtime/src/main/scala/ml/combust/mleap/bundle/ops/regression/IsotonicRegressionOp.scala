package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.regression.IsotonicRegressionModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.regression.IsotonicRegression

/**
  * Created by hollinwilkins on 12/27/16.
  */
class IsotonicRegressionOp extends MleapOp[IsotonicRegression, IsotonicRegressionModel] {
  override val Model: OpModel[MleapContext, IsotonicRegressionModel] = new OpModel[MleapContext, IsotonicRegressionModel] {
    override val klazz: Class[IsotonicRegressionModel] = classOf[IsotonicRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.isotonic_regression

    override def store(model: Model, obj: IsotonicRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("boundaries", Value.doubleList(obj.boundaries.toSeq)).
        withValue("predictions", Value.doubleList(obj.predictions)).
        withValue("isotonic", Value.boolean(obj.isotonic)).
        withValue("feature_index", obj.featureIndex.map(v => Value.long(v)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): IsotonicRegressionModel = {
      IsotonicRegressionModel(boundaries = model.value("boundaries").getDoubleList.toArray,
        predictions = model.value("predictions").getDoubleList,
        isotonic = model.value("isotonic").getBoolean,
        featureIndex = model.getValue("feature_index").map(_.getLong.toInt))
    }
  }

  override def model(node: IsotonicRegression): IsotonicRegressionModel = node.model
}
