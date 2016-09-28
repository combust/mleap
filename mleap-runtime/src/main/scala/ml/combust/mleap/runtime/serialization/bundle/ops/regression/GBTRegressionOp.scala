package ml.combust.mleap.runtime.serialization.bundle.ops.regression

import ml.bundle.dsl._
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.{BundleContext, ModelSerializer}
import ml.combust.mleap.core.regression.{DecisionTreeRegressionModel, GBTRegressionModel}
import ml.combust.mleap.runtime.transformer.regression.GBTRegression

/**
  * Created by hollinwilkins on 9/24/16.
  */
object GBTRegressionOp extends OpNode[GBTRegression, GBTRegressionModel] {
  override val Model: OpModel[GBTRegressionModel] = new OpModel[GBTRegressionModel] {
    override def opName: String = Bundle.BuiltinOps.regression.gbt_regression

    override def store(context: BundleContext, model: WritableModel, obj: GBTRegressionModel): WritableModel = {
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

    override def load(context: BundleContext, model: ReadableModel): GBTRegressionModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().asInstanceOf[DecisionTreeRegressionModel]
      }

      GBTRegressionModel(trees = models,
        treeWeights = treeWeights,
        numFeatures = numFeatures)
    }
  }

  override def name(node: GBTRegression): String = node.uid

  override def model(node: GBTRegression): GBTRegressionModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: GBTRegressionModel): GBTRegression = {
    GBTRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  /** Get the shape of the node.
    *
    * @param node node object
    * @return shape of the node
    */
  override def shape(node: GBTRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
