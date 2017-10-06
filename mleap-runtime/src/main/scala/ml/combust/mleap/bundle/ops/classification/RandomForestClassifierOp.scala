package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.classification.{DecisionTreeClassifierModel, RandomForestClassifierModel}
import ml.combust.mleap.runtime.transformer.classification.RandomForestClassifier
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.bundle.dsl._
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.bundle.tree.decision.MleapNodeWrapper
import ml.combust.mleap.runtime.frame.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
class RandomForestClassifierOp extends MleapOp[RandomForestClassifier, RandomForestClassifierModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[MleapContext, RandomForestClassifierModel] = new OpModel[MleapContext, RandomForestClassifierModel] {
    override val klazz: Class[RandomForestClassifierModel] = classOf[RandomForestClassifierModel]

    override def opName: String = Bundle.BuiltinOps.classification.random_forest_classifier

    override def store(model: Model, obj: RandomForestClassifierModel)
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
        withValue("num_classes", Value.long(obj.numClasses)).
        withValue("tree_weights", Value.doubleList(obj.treeWeights)).
        withValue("trees", Value.stringList(trees))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): RandomForestClassifierModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val numClasses = model.value("num_classes").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().get.asInstanceOf[DecisionTreeClassifierModel]
      }

      RandomForestClassifierModel(numFeatures = numFeatures,
        numClasses = numClasses,
        trees = models,
        treeWeights = treeWeights)
    }
  }

  override def model(node: RandomForestClassifier): RandomForestClassifierModel = node.model
}
