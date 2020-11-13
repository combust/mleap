package ml.combust.mleap.xgboost.runtime.bundle.ops

import java.nio.file.Files

import biz.k11i.xgboost.Predictor
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.Model
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.xgboost.runtime.{XGBoostPredictorRegression, XGBoostPredictorRegressionModel}


class XGBoostPredictorRegressionOp extends MleapOp[XGBoostPredictorRegression, XGBoostPredictorRegressionModel] {
  override val Model: OpModel[MleapContext, XGBoostPredictorRegressionModel] = new OpModel[MleapContext, XGBoostPredictorRegressionModel] {
    override val klazz: Class[XGBoostPredictorRegressionModel] = classOf[XGBoostPredictorRegressionModel]

    override def opName: String = "xgboost.regression"

    override def store(model: Model, obj: XGBoostPredictorRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      throw new RuntimeException("The XGBoostPredictor implementation does not support storing the model.")
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): XGBoostPredictorRegressionModel = {
      val predictor = new Predictor(Files.newInputStream(context.file("xgboost.model")))

      val numFeatures = model.value("num_features").getInt
      val treeLimit = model.value("tree_limit").getInt

      XGBoostPredictorRegressionModel(
        predictor,
        numFeatures,
        treeLimit = treeLimit)
    }
  }

  override def model(node: XGBoostPredictorRegression): XGBoostPredictorRegressionModel = node.model
}
