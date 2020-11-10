package ml.dmlc.xgboost4j.scala.spark.mleap

import java.nio.file.Files

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, NodeShape, Value}
import ml.combust.bundle.op.OpModel
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressionModel
import ml.dmlc.xgboost4j.scala.{XGBoost => SXGBoost}
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.linalg.Vector
import resource.managed

/**
  * Created by hollinwilkins on 9/16/17.
  */
class XGBoostRegressionModelOp extends SimpleSparkOp[XGBoostRegressionModel] {
  /** Type class for the underlying model.
    */
  override val Model: OpModel[SparkBundleContext, XGBoostRegressionModel] = new OpModel[SparkBundleContext, XGBoostRegressionModel] {
    override val klazz: Class[XGBoostRegressionModel] = classOf[XGBoostRegressionModel]

    override def opName: String = "xgboost.regression"

    override def store(model: Model, obj: XGBoostRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      Files.write(context.file("xgboost.model"), obj._booster.toByteArray)

      val numFeatures = context.context.dataset.get.select(obj.getFeaturesCol).first.getAs[Vector](0).size
      model.withValue("num_features", Value.int(numFeatures)).
        withValue("tree_limit", Value.int(obj.getOrDefault(obj.treeLimit))).
        withValue("objective", Value.string(obj.getOrDefault(obj.objective))).
        withValue("eval_metric", Value.string(obj.getOrDefault(obj.evalMetric))).
        withValue("label_col", Value.string(obj.getOrDefault(obj.labelCol))).
        withValue("missing", Value.float(obj.getOrDefault(obj.missing))).
        withValue("allow_non_zero_for_missing", Value.boolean(obj.getOrDefault(obj.allowNonZeroForMissing)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): XGBoostRegressionModel = {
      val booster = (for(in <- managed(Files.newInputStream(context.file("xgboost.model")))) yield {
        SXGBoost.loadModel(in)
      }).tried.get

      val regressor = new XGBoostRegressionModel("", booster).
        setTreeLimit(model.value("tree_limit").getInt)

      val objective = model.getValue("objective")
      if(objective.isDefined)
        regressor.set(regressor.objective, objective.get.getString)

      val evalMetric = model.getValue("eval_metric")
      if(evalMetric.isDefined)
        regressor.set(regressor.evalMetric, evalMetric.get.getString)

      val labelCol = model.getValue("label_col")
      if(labelCol.isDefined)
        regressor.set(regressor.labelCol, labelCol.get.getString)

      val missing = model.getValue("missing")
      if(missing.isDefined)
        regressor.setMissing(missing.get.getFloat)

      val allowNonZeroForMissing = model.getValue("allow_non_zero_for_missing")
      if(allowNonZeroForMissing.isDefined)
        regressor.setAllowZeroForMissingValue(allowNonZeroForMissing.get.getBoolean)

      regressor
    }
  }

  override def sparkLoad(uid: String,
                         shape: NodeShape,
                         model: XGBoostRegressionModel): XGBoostRegressionModel = {
    val regressor = new XGBoostRegressionModel(uid, model._booster).
      setMissing(model.getOrDefault(model.missing)).
      setAllowZeroForMissingValue(model.getOrDefault(model.allowNonZeroForMissing)).
      setTreeLimit(model.getOrDefault(model.treeLimit))
    regressor.set(regressor.objective, model.getOrDefault(model.objective)).
      set(regressor.evalMetric, model.getOrDefault(model.evalMetric)).
      set(regressor.labelCol, model.getOrDefault(model.labelCol))
  }

  override def sparkInputs(obj: XGBoostRegressionModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: XGBoostRegressionModel): Seq[SimpleParamSpec] = {
    Seq("prediction" -> obj.predictionCol,
      "leaf_prediction" -> obj.leafPredictionCol,
      "contrib_prediction" -> obj.contribPredictionCol)
  }
}
