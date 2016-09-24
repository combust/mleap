package ml.combust.mleap.runtime.serialization.bundle.ops.regression

import ml.combust.mleap.core.regression.{DecisionTreeRegressionModel, RandomForestRegressionModel}
import ml.combust.mleap.runtime.serialization.bundle.tree.MleapNodeWrapper
import ml.combust.mleap.runtime.transformer.regression.RandomForestRegression
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, ModelSerializer}
import ml.bundle.dsl._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object RandomForestRegressionOp extends OpNode[RandomForestRegression, RandomForestRegressionModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[RandomForestRegressionModel] = new OpModel[RandomForestRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.regression.random_forest_regression

    override def store(context: BundleContext, model: WritableModel, obj: RandomForestRegressionModel): WritableModel = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree)
          i = i + 1
          name
      }
      model.withAttr(Attribute("num_features", Value.long(obj.numFeatures))).
        withAttr(Attribute("tree_weights", Value.doubleList(obj.treeWeights))).
        withAttr(Attribute("trees", Value.stringList(trees)))
    }

    override def load(context: BundleContext, model: ReadableModel): RandomForestRegressionModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().asInstanceOf[DecisionTreeRegressionModel]
      }

      RandomForestRegressionModel(numFeatures = numFeatures,
        treeWeights = treeWeights,
        trees = models)
    }
  }

  override def name(node: RandomForestRegression): String = node.uid

  override def model(node: RandomForestRegression): RandomForestRegressionModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: RandomForestRegressionModel): RandomForestRegression = {
    RandomForestRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.input("prediction").name,
      model = model)
  }

  override def shape(node: RandomForestRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
