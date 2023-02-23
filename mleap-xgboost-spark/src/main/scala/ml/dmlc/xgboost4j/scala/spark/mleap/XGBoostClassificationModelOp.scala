package ml.dmlc.xgboost4j.scala.spark.mleap

import java.nio.file.Files

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, NodeShape, Value}
import ml.combust.bundle.op.OpModel
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel
import ml.dmlc.xgboost4j.scala.{XGBoost => SXGBoost}
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.linalg.Vector
import resource._

/**
  * Created by hollinwilkins on 9/16/17.
  */
class XGBoostClassificationModelOp extends SimpleSparkOp[XGBoostClassificationModel] {
  /** Type class for the underlying model.
    */
  override val Model: OpModel[SparkBundleContext, XGBoostClassificationModel] = new OpModel[SparkBundleContext, XGBoostClassificationModel] {
    override val klazz: Class[XGBoostClassificationModel] = classOf[XGBoostClassificationModel]

    override def opName: String = "xgboost.classifier"

    override def store(model: Model, obj: XGBoostClassificationModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val thresholds = if(obj.isSet(obj.thresholds)) {
        Some(obj.getThresholds)
      } else None

      val out = Files.newOutputStream(context.file("xgboost.model"))
      obj._booster.saveModel(out)
      val numFeatures = context.context.dataset.get.select(obj.getFeaturesCol).first.getAs[Vector](0).size
      model.withValue("thresholds", thresholds.map(_.toSeq).map(Value.doubleList)).
        withValue("num_classes", Value.int(obj.numClasses)).
        withValue("num_features", Value.int(numFeatures)).
        withValue("tree_limit", Value.int(obj.getOrDefault(obj.treeLimit))).
        withValue("missing", Value.float(obj.getOrDefault(obj.missing))).
        withValue("infer_batch_size", Value.int(obj.getOrDefault(obj.inferBatchSize))).
        withValue("use_external_memory", Value.boolean(obj.getOrDefault(obj.useExternalMemory))).
        withValue("allow_non_zero_for_missing", Value.boolean(obj.getOrDefault(obj.allowNonZeroForMissing))).
        withValue("objective", Value.string(obj.getOrDefault(obj.objective)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): XGBoostClassificationModel = {
      val booster = (for(in <- managed(Files.newInputStream(context.file("xgboost.model")))) yield {
        SXGBoost.loadModel(in)
      }).tried.get

      val xgb = new XGBoostClassificationModel("", model.value("num_classes").getInt, booster)

      model.getValue("tree_limit").map(o => xgb.setTreeLimit(o.getInt))
      model.getValue("thresholds").map(o => xgb.setThresholds(o.getDoubleList.toArray))
      model.getValue("missing").map(o => xgb.setMissing(o.getFloat))
      model.getValue("allow_non_zero_for_missing").map(o => xgb.setAllowNonZeroForMissing(o.getBoolean))
      model.getValue("infer_batch_size").map(o => xgb.setInferBatchSize(o.getInt))
      model.getValue("use_external_memory").map(o => xgb.set(xgb.useExternalMemory, o.getBoolean))
      model.getValue("objective").map(o => xgb.set(xgb.objective, o.getString))
      xgb
    }
  }

  override def sparkLoad(uid: String,
                         shape: NodeShape,
                         model: XGBoostClassificationModel): XGBoostClassificationModel = {
    val xgb = new XGBoostClassificationModel(uid, model.numClasses, model._booster)
    if(model.isSet(model.thresholds)) xgb.setThresholds(model.getOrDefault(model.thresholds))
    if(model.isSet(model.missing)) xgb.setMissing(model.getOrDefault(model.missing))
    if(model.isSet(model.allowNonZeroForMissing)) xgb.setAllowNonZeroForMissing(model.getOrDefault(model.allowNonZeroForMissing))
    if(model.isSet(model.inferBatchSize)) xgb.setInferBatchSize(model.getOrDefault(model.inferBatchSize))
    if(model.isSet(model.treeLimit)) xgb.setTreeLimit(model.getOrDefault(model.treeLimit))
    if(model.isSet(model.useExternalMemory)) xgb.set(xgb.useExternalMemory, model.getOrDefault(model.useExternalMemory))
    xgb
  }

  override def sparkInputs(obj: XGBoostClassificationModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: XGBoostClassificationModel): Seq[SimpleParamSpec] = {
    Seq("raw_prediction" -> obj.rawPredictionCol,
      "prediction" -> obj.predictionCol,
      "probability" -> obj.probabilityCol,
      "leaf_prediction" -> obj.leafPredictionCol,
      "contrib_prediction" -> obj.contribPredictionCol)
  }
}
