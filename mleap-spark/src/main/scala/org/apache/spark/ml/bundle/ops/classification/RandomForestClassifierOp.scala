package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.bundle.tree.decision.SparkNodeWrapper
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}

/**
  * Created by hollinwilkins on 8/22/16.
  */
class RandomForestClassifierOp extends SimpleSparkOp[RandomForestClassificationModel] {
  implicit val nodeWrapper = SparkNodeWrapper

  override val Model: OpModel[SparkBundleContext, RandomForestClassificationModel] = new OpModel[SparkBundleContext, RandomForestClassificationModel] {
    override val klazz: Class[RandomForestClassificationModel] = classOf[RandomForestClassificationModel]

    override def opName: String = Bundle.BuiltinOps.classification.random_forest_classifier

    override def store(model: Model, obj: RandomForestClassificationModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
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
                     (implicit context: BundleContext[SparkBundleContext]): RandomForestClassificationModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val numClasses = model.value("num_classes").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList

      // TODO: get rid of this when Spark supports setting tree weights
      for(weight <- treeWeights) { require(weight == 1.0, "tree weights must be 1.0 for Spark") }

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().get.asInstanceOf[DecisionTreeClassificationModel]
      }.toArray

      new RandomForestClassificationModel(uid = "",
        numFeatures = numFeatures,
        numClasses = numClasses,
        _trees = models)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: RandomForestClassificationModel): RandomForestClassificationModel = {
    new RandomForestClassificationModel(uid = uid,
      _trees = model.trees,
      numFeatures = model.numFeatures,
      numClasses = model.numClasses)
  }

  override def sparkInputs(obj: RandomForestClassificationModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: RandomForestClassificationModel): Seq[SimpleParamSpec] = {
    Seq("raw_prediction" -> obj.rawPredictionCol,
      "probability" -> obj.probabilityCol,
      "prediction" -> obj.predictionCol)
  }
}
