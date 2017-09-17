package ml.combust.mleap.xgboost.bundle.ops

import java.io.ByteArrayInputStream
import java.nio.file.Files

import biz.k11i.xgboost.Predictor
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.xgboost.{XGBoostBinaryClassificationModel, XGBoostClassification, XGBoostClassificationModel, XGBoostMultinomialClassificationModel}
import resource.managed

/**
  * Created by hollinwilkins on 9/16/17.
  */
class XGBoostClassificationOp extends MleapOp[XGBoostClassification, XGBoostClassificationModel] {
  override val Model: OpModel[MleapContext, XGBoostClassificationModel] = new OpModel[MleapContext, XGBoostClassificationModel] {
    override val klazz: Class[XGBoostClassificationModel] = classOf[XGBoostClassificationModel]

    override def opName: String = "xgboost.classifier"

    override def store(model: Model, obj: XGBoostClassificationModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      assert(obj.booster.isDefined, "must provide the bytes containing the booster")

      Files.write(context.file("xgboost.model"), obj.booster.get)
      model.withValue("num_features", Value.int(obj.numFeatures)).
        withValue("num_classes", Value.int(obj.numClasses)).
        withValue("output_margin", Value.boolean(obj.outputMargin))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): XGBoostClassificationModel = {
      val booster = Files.readAllBytes(context.file("xgboost.model"))
      val predictor = (for(in <- managed(new ByteArrayInputStream(booster))) yield {
        new Predictor(in)
      }).tried.get
      val numClasses = model.value("num_classes").getInt
      val numFeatures = model.value("num_features").getInt
      val outputMargin = model.value("output_margin").getBoolean

      val impl = if(numClasses == 2) {
        XGBoostBinaryClassificationModel(predictor,
          Some(booster),
          numFeatures,
          outputMargin)
      } else {
        XGBoostMultinomialClassificationModel(predictor,
          Some(booster),
          numClasses,
          numFeatures,
          outputMargin)
      }

      XGBoostClassificationModel(impl)
    }
  }

  override def model(node: XGBoostClassification): XGBoostClassificationModel = node.model
}
