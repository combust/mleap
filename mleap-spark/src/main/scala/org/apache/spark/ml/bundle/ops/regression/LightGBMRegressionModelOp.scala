package org.apache.spark.ml.bundle.ops.regression

import com.microsoft.ml.spark.lightgbm.{LightGBMBooster, LightGBMRegressionModel}
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, NodeShape, Value}
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._

class LightGBMRegressionModelOp extends SimpleSparkOp[LightGBMRegressionModel] {

  override def sparkInputs(obj: LightGBMRegressionModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }
  override def sparkOutputs(obj: LightGBMRegressionModel): Seq[ParamSpec] = {
    Seq("prediction" -> obj.predictionCol)
  }
  override def sparkLoad(
    uid: String,
    shape: NodeShape,
    model: LightGBMRegressionModel
  ): LightGBMRegressionModel = {
    val booster = new LightGBMBooster(model.getModel.model)
    new LightGBMRegressionModel(
      "",
      booster,
      model.getLabelCol,
      model.getFeaturesCol,
      model.getPredictionCol
    )
  }

  override val Model: OpModel[SparkBundleContext, LightGBMRegressionModel] =
    new OpModel[SparkBundleContext, LightGBMRegressionModel] {
      override val klazz: Class[LightGBMRegressionModel] = classOf[LightGBMRegressionModel]
      override def opName: String = "lightgbm_regression"
      override def store(model: Model, obj: LightGBMRegressionModel)(
        implicit context: BundleContext[SparkBundleContext]): Model = {
        model
          .withValue("featuresColName", Value.string(obj.getFeaturesCol))
          .withValue("predictionColName", Value.string(obj.getPredictionCol))
          .withValue("booster", Value.string(obj.getModel.model))
      }

      override def load(model: Model)(
        implicit context: BundleContext[SparkBundleContext]): LightGBMRegressionModel = {
        val booster = new LightGBMBooster(model.value("booster").getString)
        new LightGBMRegressionModel(
          "",
          booster,
          model.value("labelColName").getString,
          model.value("featuresColName").getString,
          model.value("predictionColName").getString
        )
      }
    }
}
