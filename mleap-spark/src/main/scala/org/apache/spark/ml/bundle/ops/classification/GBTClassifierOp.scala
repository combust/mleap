package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.classification.GBTClassificationModel
import org.apache.spark.ml.regression.DecisionTreeRegressionModel

/**
  * Created by hollinwilkins on 9/24/16.
  */
class GBTClassifierOp extends OpNode[SparkBundleContext, GBTClassificationModel, GBTClassificationModel] {
  override val Model: OpModel[SparkBundleContext, GBTClassificationModel] = new OpModel[SparkBundleContext, GBTClassificationModel] {
    override val klazz: Class[GBTClassificationModel] = classOf[GBTClassificationModel]

    override def opName: String = Bundle.BuiltinOps.classification.gbt_classifier

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: GBTClassificationModel): Model = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree)
          i = i + 1
          name
      }
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("num_classes", Value.long(2)).
        withAttr("tree_weights", Value.doubleList(obj.treeWeights)).
        withAttr("trees", Value.stringList(trees))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): GBTClassificationModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new IllegalArgumentException("MLeap only supports binary logistic regression")
      }

      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList.toArray

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().asInstanceOf[DecisionTreeRegressionModel]
      }.toArray

      new GBTClassificationModel(uid = "",
        _trees = models,
        _treeWeights = treeWeights,
        numFeatures = numFeatures)
    }
  }

  override val klazz: Class[GBTClassificationModel] = classOf[GBTClassificationModel]

  override def name(node: GBTClassificationModel): String = node.uid

  override def model(node: GBTClassificationModel): GBTClassificationModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: GBTClassificationModel): GBTClassificationModel = {
    new GBTClassificationModel(uid = node.name,
      _trees = model.trees,
      _treeWeights = model.treeWeights,
      numFeatures = model.numFeatures)
  }

  override def shape(node: GBTClassificationModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
