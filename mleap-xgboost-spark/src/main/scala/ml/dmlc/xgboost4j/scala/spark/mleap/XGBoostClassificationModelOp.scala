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
        withValue("objective", Value.string(obj.getOrDefault(obj.objective))).
        withValue("label_col", Value.string(obj.getOrDefault(obj.labelCol))).
        withValue("missing", Value.float(obj.getOrDefault(obj.missing))).
        withValue("eval_metric", Value.string(obj.getOrDefault(obj.evalMetric))).
        withValue("allow_non_zero_for_missing", Value.boolean(obj.getOrDefault(obj.allowNonZeroForMissing)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): XGBoostClassificationModel = {
      val booster = (for(in <- managed(Files.newInputStream(context.file("xgboost.model")))) yield {
        SXGBoost.loadModel(in)
      }).tried.get

      val classifier = new XGBoostClassificationModel("", model.value("num_classes").getInt, booster).
          setTreeLimit(model.value("tree_limit").getInt)

      val objective = model.getValue("objective")
      if(objective.isDefined)
        classifier.set(classifier.objective, objective.get.getString)

      val evalMetric = model.getValue("eval_metric")
      if(evalMetric.isDefined)
        classifier.set(classifier.evalMetric, evalMetric.get.getString)

      val labelCol = model.getValue("label_col")
      if(labelCol.isDefined)
        classifier.set(classifier.labelCol, labelCol.get.getString)

      val missing = model.getValue("missing")
      if(missing.isDefined)
        classifier.setMissing(missing.get.getFloat)

      val allowNonZeroForMissing = model.getValue("allow_non_zero_for_missing")
      if(allowNonZeroForMissing.isDefined)
        classifier.setAllowZeroForMissingValue(allowNonZeroForMissing.get.getBoolean)

      classifier
    }
  }

  override def sparkLoad(uid: String,
                         shape: NodeShape,
                         model: XGBoostClassificationModel): XGBoostClassificationModel = {
    val classifier = new XGBoostClassificationModel(uid, model.numClasses, model._booster).
      setMissing(model.getOrDefault(model.missing)).
      setAllowZeroForMissingValue(model.getOrDefault(model.allowNonZeroForMissing)).
      setTreeLimit(model.getOrDefault(model.treeLimit))
    classifier.set(classifier.objective, model.getOrDefault(model.objective)).
      set(classifier.evalMetric, model.getOrDefault(model.evalMetric)).
      set(classifier.labelCol, model.getOrDefault(model.labelCol))
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
