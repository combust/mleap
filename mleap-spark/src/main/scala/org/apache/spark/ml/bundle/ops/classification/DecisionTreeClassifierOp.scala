package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.tree.TreeSerializer
import org.apache.spark.ml.tree
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.bundle.tree.SparkNodeWrapper
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

/**
  * Created by hollinwilkins on 8/22/16.
  */
class DecisionTreeClassifierOp extends OpNode[SparkBundleContext, DecisionTreeClassificationModel, DecisionTreeClassificationModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[SparkBundleContext, DecisionTreeClassificationModel] = new OpModel[SparkBundleContext, DecisionTreeClassificationModel] {
    override val klazz: Class[DecisionTreeClassificationModel] = classOf[DecisionTreeClassificationModel]

    override def opName: String = Bundle.BuiltinOps.classification.decision_tree_classifier

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: DecisionTreeClassificationModel): Model = {
      TreeSerializer[tree.Node](context.file("nodes"), withImpurities = true).write(obj.rootNode)
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("num_classes", Value.long(obj.numClasses))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): DecisionTreeClassificationModel = {
      val rootNode = TreeSerializer[tree.Node](context.file("nodes"), withImpurities = true).read()
      new DecisionTreeClassificationModel(uid = "",
        rootNode = rootNode,
        numClasses = model.value("num_classes").getLong.toInt,
        numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override val klazz: Class[DecisionTreeClassificationModel] = classOf[DecisionTreeClassificationModel]

  override def name(node: DecisionTreeClassificationModel): String = node.uid

  override def model(node: DecisionTreeClassificationModel): DecisionTreeClassificationModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: DecisionTreeClassificationModel): DecisionTreeClassificationModel = {
    new DecisionTreeClassificationModel(uid = node.name,
      rootNode = model.rootNode,
      numClasses = model.numClasses,
      numFeatures = model.numFeatures).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: DecisionTreeClassificationModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
