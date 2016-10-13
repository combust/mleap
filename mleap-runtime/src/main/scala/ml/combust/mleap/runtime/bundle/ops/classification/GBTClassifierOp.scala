package ml.combust.mleap.runtime.bundle.ops.classification

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.{BundleContext, ModelSerializer}
import ml.combust.mleap.core.classification.GBTClassifierModel
import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.runtime.transformer.classification.GBTClassifier

/**
  * Created by hollinwilkins on 9/24/16.
  */
object GBTClassifierOp extends OpNode[GBTClassifier, GBTClassifierModel] {
  override val Model: OpModel[GBTClassifierModel] = new OpModel[GBTClassifierModel] {
    override def opName: String = Bundle.BuiltinOps.classification.gbt_classifier

    override def store(context: BundleContext, model: Model, obj: GBTClassifierModel): Model = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree)
          i = i + 1
          name
      }
      model.withAttr(Attribute("num_features", Value.long(obj.numFeatures))).
        withAttr("num_classes", Value.long(2)).
        withAttr("tree_weights", Value.doubleList(obj.treeWeights)).
        withAttr("trees", Value.stringList(trees)).
        withAttr("threshold", obj.threshold.map(Value.double))
    }

    override def load(context: BundleContext, model: Model): GBTClassifierModel = {
      if(model.value("num_classes").getLong != 2) {
        throw new Error("MLeap only supports binary logistic regression")
      } // TODO: Better error

      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList
      val threshold = model.getValue("threshold").map(_.getDouble)

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().asInstanceOf[DecisionTreeRegressionModel]
      }

      GBTClassifierModel(numFeatures = numFeatures,
        threshold = threshold,
        trees = models,
        treeWeights = treeWeights)
    }
  }

  override def name(node: GBTClassifier): String = node.uid

  override def model(node: GBTClassifier): GBTClassifierModel = node.model

  override def load(context: BundleContext, node: Node, model: GBTClassifierModel): GBTClassifier = {
    GBTClassifier(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: GBTClassifier): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
