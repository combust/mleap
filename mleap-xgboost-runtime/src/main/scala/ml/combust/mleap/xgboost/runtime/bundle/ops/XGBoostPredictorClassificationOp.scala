package ml.combust.mleap.xgboost.runtime.bundle.ops

import java.nio.file.Files

import biz.k11i.xgboost.Predictor
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.xgboost.runtime._


/**
  * Created by hollinwilkins on 9/16/17.
  */
class XGBoostPredictorClassificationOp extends MleapOp[XGBoostPredictorClassification, XGBoostPredictorClassificationModel] {

  override val Model: OpModel[MleapContext, XGBoostPredictorClassificationModel] = new OpModel[MleapContext, XGBoostPredictorClassificationModel] {
  override val klazz: Class[XGBoostPredictorClassificationModel] = classOf[XGBoostPredictorClassificationModel]

  override def opName: String = "xgboost.classifier"

  @throws[RuntimeException]
  override def store(model: Model, obj: XGBoostPredictorClassificationModel)
                    (implicit context: BundleContext[MleapContext]): Model =
    throw new RuntimeException("The XGBoostPredictor implementation does not support storing the model.")

  override def load(model: Model)
                   (implicit context: BundleContext[MleapContext]): XGBoostPredictorClassificationModel = {

    val predictor = new Predictor(Files.newInputStream(context.file("xgboost.model")))

    val numClasses = model.value("num_classes").getInt
    val numFeatures = model.value("num_features").getInt
    val treeLimit = model.value("tree_limit").getInt

    val impl = if(numClasses == 2) {
      XGBoostPredictorBinaryClassificationModel(predictor, numFeatures, treeLimit)
    } else {
      XGBoostPredictorMultinomialClassificationModel(predictor, numClasses, numFeatures, treeLimit)
    }

    XGBoostPredictorClassificationModel(impl)
  }
}

  override def model(node: XGBoostPredictorClassification): XGBoostPredictorClassificationModel = node.model
}
