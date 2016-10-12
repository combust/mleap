package ml.combust.mleap.runtime.bundle.ops.classification

import ml.combust.mleap.core.classification.{DecisionTreeClassifierModel, RandomForestClassifierModel}
import ml.combust.mleap.runtime.bundle.tree.MleapNodeWrapper
import ml.combust.mleap.runtime.transformer.classification.RandomForestClassifier
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.{BundleContext, ModelSerializer}
import ml.combust.bundle.dsl._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object RandomForestClassifierOp extends OpNode[RandomForestClassifier, RandomForestClassifierModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[RandomForestClassifierModel] = new OpModel[RandomForestClassifierModel] {
    override def opName: String = Bundle.BuiltinOps.classification.random_forest_classifier

    override def store(context: BundleContext, model: Model, obj: RandomForestClassifierModel): Model = {
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

    override def load(context: BundleContext, model: Model): RandomForestClassifierModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val numClasses = model.value("num_classes").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().asInstanceOf[DecisionTreeClassifierModel]
      }

      RandomForestClassifierModel(numFeatures = numFeatures,
        numClasses = numClasses,
        trees = models,
        treeWeights = treeWeights)
    }
  }

  override def name(node: RandomForestClassifier): String = node.uid

  override def model(node: RandomForestClassifier): RandomForestClassifierModel = node.model

  override def load(context: BundleContext, node: Node, model: RandomForestClassifierModel): RandomForestClassifier = {
    RandomForestClassifier(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.input("prediction").name,
      model = model)
  }

  override def shape(node: RandomForestClassifier): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
