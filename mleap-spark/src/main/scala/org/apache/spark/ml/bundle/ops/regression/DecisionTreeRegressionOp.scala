package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.tree.TreeSerializer
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.bundle.tree.SparkNodeWrapper
import org.apache.spark.ml.regression.DecisionTreeRegressionModel

/**
  * Created by hollinwilkins on 8/22/16.
  */
class DecisionTreeRegressionOp extends OpNode[SparkBundleContext, DecisionTreeRegressionModel, DecisionTreeRegressionModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[SparkBundleContext, DecisionTreeRegressionModel] = new OpModel[SparkBundleContext, DecisionTreeRegressionModel] {
    override val klazz: Class[DecisionTreeRegressionModel] = classOf[DecisionTreeRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.decision_tree_regression

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: DecisionTreeRegressionModel): Model = {
      TreeSerializer[org.apache.spark.ml.tree.Node](context.file("nodes"), withImpurities = false).write(obj.rootNode)
      model.withAttr("num_features", Value.long(obj.numFeatures))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): DecisionTreeRegressionModel = {
      val rootNode = TreeSerializer[org.apache.spark.ml.tree.Node](context.file("nodes"), withImpurities = false).read()
      new DecisionTreeRegressionModel(uid = "",
        rootNode = rootNode,
        numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override val klazz: Class[DecisionTreeRegressionModel] = classOf[DecisionTreeRegressionModel]

  override def name(node: DecisionTreeRegressionModel): String = node.uid

  override def model(node: DecisionTreeRegressionModel): DecisionTreeRegressionModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: DecisionTreeRegressionModel): DecisionTreeRegressionModel = {
    new DecisionTreeRegressionModel(uid = node.name,
      rootNode = model.rootNode,
      numFeatures = model.numFeatures).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: DecisionTreeRegressionModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
