package ml.combust.mleap.xgboost.runtime.bundle.ops

import java.io.ByteArrayInputStream
import java.nio.file.Files

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.xgboost.runtime.{XGBoostRegression, XGBoostRegressionModel}
import ml.dmlc.xgboost4j.scala.XGBoost
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
      val out = Files.newOutputStream(context.file("xgboost.model"))
      obj.booster.saveModel(out)

      model
        .withValue("num_features", Value.int(obj.numFeatures))
        .withValue("tree_limit", Value.int(obj.treeLimit))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): XGBoostRegressionModel = {
      val bytes = Files.readAllBytes(context.file("xgboost.model"))
      val booster = XGBoost.loadModel(new ByteArrayInputStream(bytes))
      val treeLimit = model.getValue("tree_limit")
        .map(_.getInt).getOrElse(0)

      XGBoostRegressionModel(booster,
        numFeatures = model.value("num_features").getInt,
        treeLimit = treeLimit)
    }
  }

  override def model(node: XGBoostRegression): XGBoostRegressionModel = node.model
}
