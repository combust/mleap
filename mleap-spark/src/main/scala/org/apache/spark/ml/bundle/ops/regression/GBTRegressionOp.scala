package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.{BundleContext, ModelSerializer}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, GBTRegressionModel}

/**
  * Created by hollinwilkins on 9/24/16.
  */
class GBTRegressionOp extends OpNode[SparkBundleContext, GBTRegressionModel, GBTRegressionModel] {
  override val Model: OpModel[SparkBundleContext, GBTRegressionModel] = new OpModel[SparkBundleContext, GBTRegressionModel] {
    override val klazz: Class[GBTRegressionModel] = classOf[GBTRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.gbt_regression

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: GBTRegressionModel): Model = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree)
          i = i + 1
          name
      }
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("tree_weights", Value.doubleList(obj.treeWeights)).
        withAttr("trees", Value.stringList(trees))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): GBTRegressionModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList.toArray

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().asInstanceOf[DecisionTreeRegressionModel]
      }.toArray

      new GBTRegressionModel(uid = "",
        _trees = models,
        _treeWeights = treeWeights,
        numFeatures = numFeatures)
    }
  }

  override val klazz: Class[GBTRegressionModel] = classOf[GBTRegressionModel]

  override def name(node: GBTRegressionModel): String = node.uid

  override def model(node: GBTRegressionModel): GBTRegressionModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: GBTRegressionModel): GBTRegressionModel = {
    new GBTRegressionModel(uid = node.name,
      _trees = model.trees,
      _treeWeights = model.treeWeights,
      numFeatures = model.numFeatures)
  }

  override def shape(node: GBTRegressionModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
