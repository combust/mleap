package ml.combust.mleap.bundle.ops.regression

import com.microsoft.ml.spark.lightgbm.LightGBMBooster
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.regression.LightGBMRegressionModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.regression.LightGBMRegression

class LightGBMRegressionModelOp extends MleapOp[LightGBMRegression, LightGBMRegressionModel] {
  override val Model: OpModel[MleapContext, LightGBMRegressionModel] =
    new OpModel[MleapContext, LightGBMRegressionModel] {
      override val klazz: Class[LightGBMRegressionModel] = classOf[LightGBMRegressionModel]

      override def opName: String = "lightgbm_regression"

      override def store(model: Model, obj: LightGBMRegressionModel)(
        implicit context: BundleContext[MleapContext]): Model = {
        model
          .withValue("booster", Value.string(obj.booster.model))
          .withValue("featuresColName", Value.string(obj.featuresColName))
          .withValue("predictionColName", Value.string(obj.predictionColName))
      }

      override def load(model: Model)(implicit context: BundleContext[MleapContext]): LightGBMRegressionModel =
      {
        val booster = new LightGBMBooster(model.value("booster").getString)
        new LightGBMRegressionModel(
          booster,
          model.value("featuresColName").getString,
          model.value("predictionColName").getString)
      }
    }

  override def model(node: LightGBMRegression): LightGBMRegressionModel = node.model
}
