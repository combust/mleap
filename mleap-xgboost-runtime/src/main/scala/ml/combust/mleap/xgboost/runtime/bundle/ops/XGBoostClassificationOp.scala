package ml.combust.mleap.xgboost.runtime.bundle.ops

import java.io.FileInputStream
import java.nio.file.Files

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.xgboost.runtime.{XGBoostBinaryClassificationModel, XGBoostClassification, XGBoostClassificationModel, XGBoostMultinomialClassificationModel, XGBoostPerformantBinaryClassificationModel, XGBoostPerformantClassification, XGBoostPerformantClassificationModel}
import ml.dmlc.xgboost4j.scala.{Booster, XGBoost}
import biz.k11i.xgboost.Predictor

//import biz.k11i.xgboost.util.FVec;


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
      model
        .withValue("num_features", Value.int(obj.numFeatures))
        .withValue("num_classes", Value.int(obj.numClasses))
        .withValue("tree_limit", Value.int(obj.treeLimit))
    }

    override def load(model: Model)
            (implicit context: BundleContext[MleapContext]): XGBoostClassificationModel = {

      val booster: Booster = XGBoost.loadModel(Files.newInputStream(context.file("xgboost.model")))

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


  def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): XGBoostPerformantClassificationModel = {

      val predictor = new Predictor(new FileInputStream("/path/to/xgboost-model-file"))

      val numClasses = model.value("num_classes").getInt
      val numFeatures = model.value("num_features").getInt
      val treeLimit = model.value("tree_limit").getInt

      val impl = XGBoostPerformantBinaryClassificationModel(predictor, numFeatures, treeLimit)

      XGBoostPerformantClassificationModel(impl)
    }

  override def model(node: XGBoostClassification): XGBoostClassificationModel = node.model
}
