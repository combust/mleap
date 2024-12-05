package org.apache.spark.ml.bundle.ops.classification

import com.microsoft.ml.spark.lightgbm.{LightGBMBooster, LightGBMClassificationModel}
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, NodeShape, Value}
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._

class LightGBMClassificationModelOp
    extends SimpleSparkOp[LightGBMClassificationModel] {
  override def sparkInputs(obj: LightGBMClassificationModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }
  override def sparkOutputs(obj: LightGBMClassificationModel): Seq[ParamSpec] = {
    Seq(
      "raw_prediction" -> obj.rawPredictionCol,
      "probability" -> obj.probabilityCol,
      "prediction" -> obj.predictionCol
    )
  }
  override def sparkLoad(
    uid: String,
    shape: NodeShape,
    model: LightGBMClassificationModel
  ): LightGBMClassificationModel = {
    val booster = new LightGBMBooster(model.getModel.model)
    new LightGBMClassificationModel(
      "",
      booster,
      model.getLabelCol,
      model.getFeaturesCol,
      model.getPredictionCol,
      model.getProbabilityCol,
      model.getRawPredictionCol,
      Some(model.getThresholds),
      model.numClasses
    )
  }

  override val Model: OpModel[SparkBundleContext, LightGBMClassificationModel] =
    new OpModel[SparkBundleContext, LightGBMClassificationModel] {
      override val klazz: Class[LightGBMClassificationModel] =
        classOf[LightGBMClassificationModel]
      override def opName: String = "lightgbm_classifier"
      override def store(model: Model, obj: LightGBMClassificationModel)(
        implicit context: BundleContext[SparkBundleContext]
      ): Model = {
        assert(
          context.context.dataset.isDefined,
          BundleHelper.sampleDataframeMessage(klazz)
        )
        val thresholds = if (obj.isSet(obj.thresholds)) {
          Some(obj.getThresholds)
        } else None

        model
          .withValue("booster", Value.string(obj.getModel.model))
          .withValue("labelColName", Value.string(obj.getLabelCol))
          .withValue("featuresColName", Value.string(obj.getFeaturesCol))
          .withValue("predictionColName", Value.string(obj.getPredictionCol))
          .withValue("probColName", Value.string(obj.getProbabilityCol))
          .withValue("rawPredictionColName", Value.string(obj.getRawPredictionCol))
          .withValue("thresholdValues", thresholds.map(_.toSeq).map(Value.doubleList))
          .withValue("actualNumClasses", Value.int(obj.numClasses))
      }

      override def load(model: Model)(
        implicit context: BundleContext[SparkBundleContext]
      ): LightGBMClassificationModel = {
        val booster = new LightGBMBooster(model.value("booster").getString)
        new LightGBMClassificationModel(
          "",
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
}
