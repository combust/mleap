package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.tree.TreeSerializer
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.tree.SparkNodeWrapper
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

/**
  * Created by hollinwilkins on 8/22/16.
  */
object DecisionTreeClassifierOp extends OpNode[DecisionTreeClassificationModel, DecisionTreeClassificationModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[DecisionTreeClassificationModel] = new OpModel[DecisionTreeClassificationModel] {
    override def opName: String = Bundle.BuiltinOps.classification.decision_tree_classifier

    override def store(context: BundleContext, model: WritableModel, obj: DecisionTreeClassificationModel): WritableModel = {
      TreeSerializer[org.apache.spark.ml.tree.Node](context.file("nodes"), withImpurities = true).write(obj.rootNode)
      model.withAttr(Attribute("num_features", Value.long(obj.numFeatures))).
        withAttr(Attribute("num_classes", Value.long(obj.numClasses)))
    }

    override def load(context: BundleContext, model: ReadableModel): DecisionTreeClassificationModel = {
      val rootNode = TreeSerializer[org.apache.spark.ml.tree.Node](context.file("nodes"), withImpurities = true).read()
      new DecisionTreeClassificationModel(uid = "",
        rootNode = rootNode,
        numClasses = model.value("num_classes").getLong.toInt,
        numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override def name(node: DecisionTreeClassificationModel): String = node.uid

  override def model(node: DecisionTreeClassificationModel): DecisionTreeClassificationModel = node

  override def load(context: BundleContext, node: ReadableNode, model: DecisionTreeClassificationModel): DecisionTreeClassificationModel = {
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
