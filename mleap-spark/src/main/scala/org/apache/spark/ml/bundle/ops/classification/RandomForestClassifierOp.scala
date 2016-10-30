package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.bundle.tree.SparkNodeWrapper
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}

/**
  * Created by hollinwilkins on 8/22/16.
  */
class RandomForestClassifierOp extends OpNode[SparkBundleContext, RandomForestClassificationModel, RandomForestClassificationModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[SparkBundleContext, RandomForestClassificationModel] = new OpModel[SparkBundleContext, RandomForestClassificationModel] {
    override val klazz: Class[RandomForestClassificationModel] = classOf[RandomForestClassificationModel]

    override def opName: String = Bundle.BuiltinOps.classification.random_forest_classifier

    override def store(model: Model, obj: RandomForestClassificationModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree)
          i = i + 1
          name
      }
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("num_classes", Value.long(obj.numClasses)).
        withAttr("tree_weights", Value.doubleList(obj.treeWeights)).
        withAttr("trees", Value.stringList(trees))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): RandomForestClassificationModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val numClasses = model.value("num_classes").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList

      // TODO: get rid of this when Spark supports setting tree weights
      for(weight <- treeWeights) { require(weight == 1.0, "tree weights must be 1.0 for Spark") }

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().asInstanceOf[DecisionTreeClassificationModel]
      }.toArray

      new RandomForestClassificationModel(uid = "",
        numFeatures = numFeatures,
        numClasses = numClasses,
        _trees = models)
    }
  }

  override val klazz: Class[RandomForestClassificationModel] = classOf[RandomForestClassificationModel]

  override def name(node: RandomForestClassificationModel): String = node.uid

  override def model(node: RandomForestClassificationModel): RandomForestClassificationModel = node

  override def load(node: Node, model: RandomForestClassificationModel)
                   (implicit context: BundleContext[SparkBundleContext]): RandomForestClassificationModel = {
    new RandomForestClassificationModel(uid = node.name,
      numClasses = model.numClasses,
      numFeatures = model.numFeatures,
      _trees = model.trees).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.input("prediction").name)
  }

  override def shape(node: RandomForestClassificationModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
