package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.mleap.core.classification.GBTClassifierModel
import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.classification.GBTClassifier

/**
  * Created by hollinwilkins on 9/24/16.
  */
class GBTClassifierOp extends OpNode[MleapContext, GBTClassifier, GBTClassifierModel] {
  override val Model: OpModel[MleapContext, GBTClassifierModel] = new OpModel[MleapContext, GBTClassifierModel] {
    override val klazz: Class[GBTClassifierModel] = classOf[GBTClassifierModel]

    override def opName: String = Bundle.BuiltinOps.classification.gbt_classifier

    override def store(model: Model, obj: GBTClassifierModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree).get
          i = i + 1
          name
      }
      model.withValue("num_features", Value.long(obj.numFeatures)).
        withValue("num_classes", Value.long(2)).
        withValue("tree_weights", Value.doubleList(obj.treeWeights)).
        withValue("trees", Value.stringList(trees))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): GBTClassifierModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new IllegalArgumentException("MLeap only supports binary logistic regression")
      }

      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().get.asInstanceOf[DecisionTreeRegressionModel]
      }

      GBTClassifierModel(numFeatures = numFeatures,
        trees = models,
        treeWeights = treeWeights)
    }
  }

  override val klazz: Class[GBTClassifier] = classOf[GBTClassifier]

  override def name(node: GBTClassifier): String = node.uid

  override def model(node: GBTClassifier): GBTClassifierModel = node.model

  override def load(node: Node, model: GBTClassifierModel)
                   (implicit context: BundleContext[MleapContext]): GBTClassifier = {
    GBTClassifier(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: GBTClassifier): NodeShape = NodeShape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
