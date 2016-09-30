package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.{BundleContext, ModelSerializer}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.tree.SparkNodeWrapper
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}

/**
  * Created by hollinwilkins on 8/22/16.
  */
object RandomForestClassifierOp extends OpNode[RandomForestClassificationModel, RandomForestClassificationModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[RandomForestClassificationModel] = new OpModel[RandomForestClassificationModel] {
    override def opName: String = Bundle.BuiltinOps.regression.random_forest_regression

    override def store(context: BundleContext, model: Model, obj: RandomForestClassificationModel): Model = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree)
          i = i + 1
          name
      }
      model.withAttr(Attribute("num_features", Value.long(obj.numFeatures))).
        withAttr(Attribute("num_classes", Value.long(obj.numClasses))).
        withAttr(Attribute("trees", Value.stringList(trees)))
    }

    override def load(context: BundleContext, model: Model): RandomForestClassificationModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val numClasses = model.value("num_classes").getLong.toInt

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().asInstanceOf[DecisionTreeClassificationModel]
      }.toArray

      new RandomForestClassificationModel(uid = "",
        numFeatures = numFeatures,
        numClasses = numClasses,
        _trees = models)
    }
  }

  override def name(node: RandomForestClassificationModel): String = node.uid

  override def model(node: RandomForestClassificationModel): RandomForestClassificationModel = node

  override def load(context: BundleContext, node: Node, model: RandomForestClassificationModel): RandomForestClassificationModel = {
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
