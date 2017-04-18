package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.tree
import ml.combust.bundle.dsl._
import ml.combust.bundle.tree.decision.TreeSerializer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.bundle.tree.decision.SparkNodeWrapper
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

/**
  * Created by hollinwilkins on 8/22/16.
  */
class DecisionTreeClassifierOp extends OpNode[SparkBundleContext, DecisionTreeClassificationModel, DecisionTreeClassificationModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[SparkBundleContext, DecisionTreeClassificationModel] = new OpModel[SparkBundleContext, DecisionTreeClassificationModel] {
    override val klazz: Class[DecisionTreeClassificationModel] = classOf[DecisionTreeClassificationModel]

    override def opName: String = Bundle.BuiltinOps.classification.decision_tree_classifier

    override def store(model: Model, obj: DecisionTreeClassificationModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      TreeSerializer[tree.Node](context.file("tree"), withImpurities = true).write(obj.rootNode)
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("num_classes", Value.long(obj.numClasses))
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

  override val klazz: Class[DecisionTreeClassificationModel] = classOf[DecisionTreeClassificationModel]

  override def name(node: DecisionTreeClassificationModel): String = node.uid

  override def model(node: DecisionTreeClassificationModel): DecisionTreeClassificationModel = node

  override def load(node: Node, model: DecisionTreeClassificationModel)
                   (implicit context: BundleContext[SparkBundleContext]): DecisionTreeClassificationModel = {
    var dt = new DecisionTreeClassificationModel(uid = node.name,
      rootNode = model.rootNode,
      numClasses = model.numClasses,
      numFeatures = model.numFeatures).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
    dt = node.shape.getOutput("probability").map(p => dt.setProbabilityCol(p.name)).getOrElse(dt)
    node.shape.getOutput("raw_prediction").map(rp => dt.setRawPredictionCol(rp.name)).getOrElse(dt)
  }

  override def shape(node: DecisionTreeClassificationModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val rawPrediction = if(node.isDefined(node.rawPredictionCol)) Some(node.getRawPredictionCol) else None
    val probability = if(node.isDefined(node.probabilityCol)) Some(node.getProbabilityCol) else None

    Shape().withInput(node.getFeaturesCol, "features").
      withOutput(node.getPredictionCol, "prediction").
      withOutput(rawPrediction, "raw_prediction").
      withOutput(probability, "probability")
  }
}
