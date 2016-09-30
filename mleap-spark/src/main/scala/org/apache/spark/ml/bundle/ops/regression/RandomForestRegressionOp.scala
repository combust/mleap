package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.{BundleContext, ModelSerializer}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.tree.SparkNodeWrapper
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, RandomForestRegressionModel}

/**
  * Created by hollinwilkins on 8/22/16.
  */
object RandomForestRegressionOp extends OpNode[RandomForestRegressionModel, RandomForestRegressionModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[RandomForestRegressionModel] = new OpModel[RandomForestRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.regression.random_forest_regression

    override def store(context: BundleContext, model: Model, obj: RandomForestRegressionModel): Model = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree)
          i = i + 1
          name
      }
      model.withAttr(Attribute("num_features", Value.long(obj.numFeatures))).
        withAttr(Attribute("trees", Value.stringList(trees)))
    }

    override def load(context: BundleContext, model: Model): RandomForestRegressionModel = {
      val numFeatures = model.value("num_features").getLong.toInt

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().asInstanceOf[DecisionTreeRegressionModel]
      }.toArray

      new RandomForestRegressionModel(uid = "",
        numFeatures = numFeatures,
        _trees = models)
    }
  }

  override def name(node: RandomForestRegressionModel): String = node.uid

  override def model(node: RandomForestRegressionModel): RandomForestRegressionModel = node

  override def load(context: BundleContext, node: Node, model: RandomForestRegressionModel): RandomForestRegressionModel = {
    new RandomForestRegressionModel(uid = node.name,
      numFeatures = model.numFeatures,
      _trees = model.trees).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.input("prediction").name)
  }

  override def shape(node: RandomForestRegressionModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
