package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.tree
import ml.combust.bundle.dsl._
import ml.combust.bundle.tree.decision.TreeSerializer
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.bundle.tree.decision.SparkNodeWrapper
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

/**
  * Created by hollinwilkins on 8/22/16.
  */
class DecisionTreeClassifierOp extends SimpleSparkOp[DecisionTreeClassificationModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[SparkBundleContext, DecisionTreeClassificationModel] = new OpModel[SparkBundleContext, DecisionTreeClassificationModel] {
    override val klazz: Class[DecisionTreeClassificationModel] = classOf[DecisionTreeClassificationModel]

    override def opName: String = Bundle.BuiltinOps.classification.decision_tree_classifier

    override def store(model: Model, obj: DecisionTreeClassificationModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      TreeSerializer[tree.Node](context.file("tree"), withImpurities = true).write(obj.rootNode)
      model.withValue("num_features", Value.long(obj.numFeatures)).
        withValue("num_classes", Value.long(obj.numClasses))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): DecisionTreeClassificationModel = {
      val rootNode = TreeSerializer[tree.Node](context.file("tree"), withImpurities = true).read().get
      new DecisionTreeClassificationModel(uid = "",
        rootNode = rootNode,
        numClasses = model.value("num_classes").getLong.toInt,
        numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: DecisionTreeClassificationModel): DecisionTreeClassificationModel = {
    val r = new DecisionTreeClassificationModel(uid = uid,
      rootNode = model.rootNode,
      numFeatures = model.numFeatures,
      numClasses = model.numClasses)
    if(model.isDefined(model.thresholds)) { r.setThresholds(model.getThresholds) }
    r
  }

  override def sparkInputs(obj: DecisionTreeClassificationModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: DecisionTreeClassificationModel): Seq[SimpleParamSpec] = {
    Seq("raw_prediction" -> obj.rawPredictionCol,
      "probability" -> obj.probabilityCol,
      "prediction" -> obj.predictionCol)
  }
}
