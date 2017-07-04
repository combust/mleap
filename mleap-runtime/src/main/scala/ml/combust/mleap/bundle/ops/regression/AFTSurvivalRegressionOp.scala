package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.regression.AFTSurvivalRegressionModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.regression.AFTSurvivalRegression
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/28/16.
  */
class AFTSurvivalRegressionOp extends MleapOp[AFTSurvivalRegression, AFTSurvivalRegressionModel] {
  override val Model: OpModel[MleapContext, AFTSurvivalRegressionModel] = new OpModel[MleapContext, AFTSurvivalRegressionModel] {
    override val klazz: Class[AFTSurvivalRegressionModel] = classOf[AFTSurvivalRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.aft_survival_regression

    override def store(model: Model, obj: AFTSurvivalRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("coefficients", Value.vector(obj.coefficients.toArray)).
        withValue("intercept", Value.double(obj.intercept)).
        withValue("quantile_probabilities", Value.doubleList(obj.quantileProbabilities)).
        withValue("scale", Value.double(obj.scale))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): AFTSurvivalRegressionModel = {
      AFTSurvivalRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble,
        quantileProbabilities = model.value("quantile_probabilities").getDoubleList.toArray,
        scale = model.value("scale").getDouble)
    }
  }

  override def model(node: AFTSurvivalRegression): AFTSurvivalRegressionModel = node.model
}
