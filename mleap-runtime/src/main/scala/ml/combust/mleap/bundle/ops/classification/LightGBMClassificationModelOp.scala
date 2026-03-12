package ml.combust.mleap.bundle.ops.classification

import com.microsoft.ml.spark.lightgbm.LightGBMBooster
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.classification.LightGBMClassifierModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.classification.LightGBMClassifier

class LightGBMClassificationModelOp extends MleapOp[LightGBMClassifier, LightGBMClassifierModel] {
  override val Model: OpModel[MleapContext, LightGBMClassifierModel] =
    new OpModel[MleapContext, LightGBMClassifierModel] {
      override val klazz: Class[LightGBMClassifierModel] = classOf[LightGBMClassifierModel]

      override def opName: String = "lightgbm_classifier"

      override def store(model: Model, obj: LightGBMClassifierModel)(
        implicit context: BundleContext[MleapContext]): Model = {
        model
          .withValue("booster", Value.string(obj.booster.model))
          .withValue("labelColName", Value.string(obj.labelColName))
          .withValue("featuresColName", Value.string(obj.featuresColName))
          .withValue("predictionColName", Value.string(obj.predictionColName))
          .withValue("probColName", Value.string(obj.probColName))
          .withValue(
            "rawPredictionColName",
            Value.string(obj.rawPredictionColName)
          )
          .withValue(
            "thresholdValues",
            obj.thresholds.map(_.toSeq).map(Value.doubleList)
          ).withValue("actualNumClasses", Value.int(obj.numClasses))
      }

      override def load(model: Model)(implicit context: BundleContext[MleapContext]): LightGBMClassifierModel =
      {
        val booster = new LightGBMBooster(model.value("booster").getString)
        new LightGBMClassifierModel(
          booster,
          model.value("labelColName").getString,
          model.value("featuresColName").getString,
          model.value("predictionColName").getString,
          model.value("probColName").getString,
          model.value("rawPredictionColName").getString,
          model.getValue("thresholdValues").map(_.getDoubleList.toArray),
          model.value("actualNumClasses").getInt
        )

      }
    }

  override def model(node: LightGBMClassifier): LightGBMClassifierModel = node.model
}
