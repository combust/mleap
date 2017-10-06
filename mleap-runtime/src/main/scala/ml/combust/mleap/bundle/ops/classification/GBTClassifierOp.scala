package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.classification.GBTClassifierModel
import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.classification.GBTClassifier

/**
  * Created by hollinwilkins on 9/24/16.
  */
class GBTClassifierOp extends MleapOp[GBTClassifier, GBTClassifierModel] {
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
        withValue("trees", Value.stringList(trees)).
        withValue("thresholds", obj.thresholds.map(Value.doubleList(_)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): GBTClassifierModel = {
      val numClasses = model.value("num_classes").getLong
      if (numClasses != 2) {
        throw new IllegalArgumentException("MLeap only supports binary logistic regression")
      }

      model.getValue("lossType").map(_.getString) match {
        case Some(lt) => require(lt == "logistic", s"MLeap only supports 'logistic' loss_type, $lt was passed to the model")
        case _ =>
      }

      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().get.asInstanceOf[DecisionTreeRegressionModel]
      }

      val thresholds = model.getValue("thresholds").map(_.getDoubleList.toArray)
      require(thresholds.isEmpty || thresholds.get.length == numClasses,
        "GBTClassifierModel loaded with non-matching numClasses and thresholds.length. " +
          s" numClasses=$numClasses, but thresholds has length ${thresholds.get.length}")

      GBTClassifierModel(numFeatures = numFeatures,
        trees = models,
        treeWeights = treeWeights,
        thresholds = thresholds)
    }
  }

  override def model(node: GBTClassifier): GBTClassifierModel = node.model
}
