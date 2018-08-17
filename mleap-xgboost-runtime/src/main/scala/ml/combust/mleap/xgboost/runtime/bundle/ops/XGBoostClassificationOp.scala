package ml.combust.mleap.xgboost.runtime.bundle.ops

import java.nio.file.Files

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.xgboost.runtime.{XGBoostBinaryClassificationModel, XGBoostClassification, XGBoostClassificationModel, XGBoostMultinomialClassificationModel}
import ml.dmlc.xgboost4j.scala.XGBoost

/**
  * Created by hollinwilkins on 9/16/17.
  */
class XGBoostClassificationOp extends MleapOp[XGBoostClassification, XGBoostClassificationModel] {
  override val Model: OpModel[MleapContext, XGBoostClassificationModel] = new OpModel[MleapContext, XGBoostClassificationModel] {
    override val klazz: Class[XGBoostClassificationModel] = classOf[XGBoostClassificationModel]

    override def opName: String = "xgboost.classifier"

    override def store(model: Model, obj: XGBoostClassificationModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val out = Files.newOutputStream(context.file("xgboost.model"))
      obj.booster.saveModel(out)
      model.withValue("num_features", Value.int(obj.numFeatures)).
        withValue("num_classes", Value.int(obj.numClasses))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): XGBoostClassificationModel = {
      val booster = XGBoost.loadModel(Files.newInputStream(context.file("xgboost.model")))
      val numClasses = model.value("num_classes").getInt
      val numFeatures = model.value("num_features").getInt
      val treeLimit = model.value("tree_limit").getInt

      val impl = if(numClasses == 2) {
        XGBoostBinaryClassificationModel(booster, numFeatures, treeLimit)
      } else {
        XGBoostMultinomialClassificationModel(booster, numClasses, numFeatures, treeLimit)
      }

      XGBoostClassificationModel(impl)
    }
  }

  override def model(node: XGBoostClassification): XGBoostClassificationModel = node.model
}
