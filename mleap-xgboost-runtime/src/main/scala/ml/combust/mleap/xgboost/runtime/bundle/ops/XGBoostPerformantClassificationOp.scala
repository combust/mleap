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
class XGBoostPerformantClassificationOp extends MleapOp[XGBoostPerformantClassification, XGBoostPerformantClassificationModel] {

  override val Model: OpModel[MleapContext, XGBoostPerformantClassificationModel] = new OpModel[MleapContext, XGBoostPerformantClassificationModel] {
  override val klazz: Class[XGBoostPerformantClassificationModel] = classOf[XGBoostPerformantClassificationModel]

  override def opName: String = "xgboost.classifier"

  @throws[RuntimeException]
  override def store(model: Model, obj: XGBoostPerformantClassificationModel)
                    (implicit context: BundleContext[MleapContext]): Model =
    throw new RuntimeException("The XGBoostPredictor implementation does not support storing the model.")

  override def load(model: Model)
                   (implicit context: BundleContext[MleapContext]): XGBoostPerformantClassificationModel = {

    val predictor = new Predictor(Files.newInputStream(context.file("xgboost.model")))

    val numClasses = model.value("num_classes").getInt
    val numFeatures = model.value("num_features").getInt
    val treeLimit = model.value("tree_limit").getInt

    val impl = XGBoostPerformantBinaryClassificationModel(predictor, numFeatures, treeLimit)

    XGBoostPerformantClassificationModel(impl)
  }
}

  override def model(node: XGBoostPerformantClassification): XGBoostPerformantClassificationModel = node.model
}
