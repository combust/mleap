package ml.combust.mleap.runtime.bundle.ops.regression

import ml.combust.mleap.core.regression.{DecisionTreeRegressionModel, RandomForestRegressionModel}
import ml.combust.mleap.runtime.bundle.tree.MleapNodeWrapper
import ml.combust.mleap.runtime.transformer.regression.RandomForestRegression
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.{BundleContext, ModelSerializer}
import ml.combust.bundle.dsl._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object RandomForestRegressionOp extends OpNode[RandomForestRegression, RandomForestRegressionModel] {
  implicit val nodeWrapper = MleapNodeWrapper

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
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("tree_weights", Value.doubleList(obj.treeWeights)).
        withAttr("trees", Value.stringList(trees))
    }

    override def load(context: BundleContext, model: Model): RandomForestRegressionModel = {
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

  override def load(context: BundleContext, node: Node, model: RandomForestRegressionModel): RandomForestRegression = {
    RandomForestRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.input("prediction").name,
      model = model)
  }

  override def shape(node: RandomForestRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
