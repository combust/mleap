package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.classification.GBTClassificationModel
import org.apache.spark.ml.regression.DecisionTreeRegressionModel

/**
  * Created by hollinwilkins on 9/24/16.
  */
class GBTClassifierOpV22 extends SimpleSparkOp[GBTClassificationModel] {
  override val Model: OpModel[SparkBundleContext, GBTClassificationModel] = new OpModel[SparkBundleContext, GBTClassificationModel] {
    override val klazz: Class[GBTClassificationModel] = classOf[GBTClassificationModel]

    override def opName: String = Bundle.BuiltinOps.classification.gbt_classifier

    override def store(model: Model, obj: GBTClassificationModel)
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
        withValue("num_classes", Value.long(2)).
        withValue("tree_weights", Value.doubleList(obj.treeWeights)).
        withValue("trees", Value.stringList(trees)).
        withValue("thresholds", obj.get(obj.thresholds).map(Value.doubleList(_)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): GBTClassificationModel = {
      if (model.value("num_classes").getLong != 2) {
        throw new IllegalArgumentException("MLeap only supports binary logistic regression")
      }

      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList.toArray

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().get.asInstanceOf[DecisionTreeRegressionModel]
      }.toArray

      val gbt = new GBTClassificationModel(uid = "",
        _trees = models,
        _treeWeights = treeWeights,
        numFeatures = numFeatures)

      model.getValue("thresholds")
        .map(t => gbt.setThresholds(t.getDoubleList.toArray))
        .getOrElse(gbt)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: GBTClassificationModel): GBTClassificationModel = {
    new GBTClassificationModel(uid = uid,
      _trees = model.trees,
      _treeWeights = model.treeWeights,
      numFeatures = model.numFeatures)
  }

  override def sparkInputs(obj: GBTClassificationModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: GBTClassificationModel): Seq[SimpleParamSpec] = {
    Seq("raw_prediction" -> obj.rawPredictionCol,
      "probability" -> obj.probabilityCol,
      "prediction" -> obj.predictionCol)
  }
}
