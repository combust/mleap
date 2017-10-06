package ml.combust.mleap.xgboost.bundle.ops

import java.io.ByteArrayInputStream
import java.nio.file.Files

import biz.k11i.xgboost.Predictor
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.xgboost.{XGBoostRegression, XGBoostRegressionModel}
import resource._

/**
  * Created by hollinwilkins on 9/16/17.
  */
class XGBoostRegressionOp extends MleapOp[XGBoostRegression, XGBoostRegressionModel] {
  override val Model: OpModel[MleapContext, XGBoostRegressionModel] = new OpModel[MleapContext, XGBoostRegressionModel] {
    override val klazz: Class[XGBoostRegressionModel] = classOf[XGBoostRegressionModel]

    override def opName: String = "xgboost.regression"

    override def store(model: Model, obj: XGBoostRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      assert(obj.booster.isDefined, "must provide the bytes containing the booster")

      Files.write(context.file("xgboost.model"), obj.booster.get)
      model.withValue("num_features", Value.int(obj.numFeatures))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): XGBoostRegressionModel = {
      val booster = Files.readAllBytes(context.file("xgboost.model"))
      val predictor = (for(in <- managed(new ByteArrayInputStream(booster))) yield {
        new Predictor(in)
      }).tried.get

      XGBoostRegressionModel(predictor,
        Some(booster),
        numFeatures = model.value("num_features").getInt)
    }
  }

  override def model(node: XGBoostRegression): XGBoostRegressionModel = node.model
}
