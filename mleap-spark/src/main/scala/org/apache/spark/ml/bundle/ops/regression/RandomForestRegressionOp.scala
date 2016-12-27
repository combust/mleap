package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.bundle.tree.decision.SparkNodeWrapper
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, RandomForestRegressionModel}

/**
  * Created by hollinwilkins on 8/22/16.
  */
class RandomForestRegressionOp extends OpNode[SparkBundleContext, RandomForestRegressionModel, RandomForestRegressionModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[SparkBundleContext, RandomForestRegressionModel] = new OpModel[SparkBundleContext, RandomForestRegressionModel] {
    override val klazz: Class[RandomForestRegressionModel] = classOf[RandomForestRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.random_forest_regression

    override def store(model: Model, obj: RandomForestRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree).get
          i = i + 1
          name
      }
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("tree_weights", Value.doubleList(obj.treeWeights)).
        withAttr("trees", Value.stringList(trees))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): RandomForestRegressionModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList

      // TODO: get rid of this when Spark supports setting tree weights
      for(weight <- treeWeights) { require(weight == 1.0, "tree weights must be 1.0 for Spark") }

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().get.asInstanceOf[DecisionTreeRegressionModel]
      }.toArray

      new RandomForestRegressionModel(uid = "",
        numFeatures = numFeatures,
        _trees = models)
    }
  }

  override val klazz: Class[RandomForestRegressionModel] = classOf[RandomForestRegressionModel]

  override def name(node: RandomForestRegressionModel): String = node.uid

  override def model(node: RandomForestRegressionModel): RandomForestRegressionModel = node

  override def load(node: Node, model: RandomForestRegressionModel)
                   (implicit context: BundleContext[SparkBundleContext]): RandomForestRegressionModel = {
    new RandomForestRegressionModel(uid = node.name,
      numFeatures = model.numFeatures,
      _trees = model.trees).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: RandomForestRegressionModel): Shape = Shape().
    withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
