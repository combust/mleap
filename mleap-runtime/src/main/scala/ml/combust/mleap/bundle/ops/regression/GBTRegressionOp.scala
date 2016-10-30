package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.mleap.core.regression.{DecisionTreeRegressionModel, GBTRegressionModel}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.regression.GBTRegression

/**
  * Created by hollinwilkins on 9/24/16.
  */
class GBTRegressionOp extends OpNode[MleapContext, GBTRegression, GBTRegressionModel] {
  override val Model: OpModel[MleapContext, GBTRegressionModel] = new OpModel[MleapContext, GBTRegressionModel] {
    override val klazz: Class[GBTRegressionModel] = classOf[GBTRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.gbt_regression

    override def store(model: Model, obj: GBTRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
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

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): GBTRegressionModel = {
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

  override val klazz: Class[GBTRegression] = classOf[GBTRegression]

  override def name(node: GBTRegression): String = node.uid

  override def model(node: GBTRegression): GBTRegressionModel = node.model

  override def load(node: Node, model: GBTRegressionModel)
                   (implicit context: BundleContext[MleapContext]): GBTRegression = {
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
