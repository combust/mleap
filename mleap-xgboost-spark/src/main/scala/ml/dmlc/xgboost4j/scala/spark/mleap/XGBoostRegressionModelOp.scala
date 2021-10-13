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
        withValue("missing", Value.float(obj.getOrDefault(obj.missing))).
        withValue("infer_batch_size", Value.int(obj.getOrDefault(obj.inferBatchSize))).
        withValue("use_external_memory", Value.boolean(obj.getOrDefault(obj.useExternalMemory))).
        withValue("allow_non_zero_for_missing", Value.boolean(obj.getOrDefault(obj.allowNonZeroForMissing)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): XGBoostRegressionModel = {
      val booster = (for(in <- managed(Files.newInputStream(context.file("xgboost.model")))) yield {
        SXGBoost.loadModel(in)
      }).tried.get

      val xgb = new XGBoostRegressionModel("", booster).
        setTreeLimit(model.value("tree_limit").getInt)

      model.getValue("missing").map(o => xgb.setMissing(o.getFloat))
      model.getValue("allow_non_zero_for_missing").map(o => xgb.setAllowNonZeroForMissing(o.getBoolean))
      model.getValue("infer_batch_size").map(o => xgb.setInferBatchSize(o.getInt))
      model.getValue("use_external_memory").map(o => xgb.set(xgb.useExternalMemory, o.getBoolean))
      xgb
    }
  }

  override def sparkLoad(uid: String,
                         shape: NodeShape,
                         model: XGBoostRegressionModel): XGBoostRegressionModel = {
    val xgb = new XGBoostRegressionModel(uid, model._booster)
    if(model.isSet(model.missing)) xgb.setMissing(model.getOrDefault(model.missing))
    if(model.isSet(model.allowNonZeroForMissing)) xgb.setAllowNonZeroForMissing(model.getOrDefault(model.allowNonZeroForMissing))
    if(model.isSet(model.inferBatchSize)) xgb.setInferBatchSize(model.getOrDefault(model.inferBatchSize))
    if(model.isSet(model.treeLimit)) xgb.setTreeLimit(model.getOrDefault(model.treeLimit))
    if(model.isSet(model.useExternalMemory)) xgb.set(xgb.useExternalMemory, model.getOrDefault(model.useExternalMemory))
    xgb
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
