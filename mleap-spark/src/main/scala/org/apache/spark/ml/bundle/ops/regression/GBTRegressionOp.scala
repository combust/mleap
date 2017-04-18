package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, GBTRegressionModel}
import org.apache.spark.sql.mleap.TypeConverters.fieldType

/**
  * Created by hollinwilkins on 9/24/16.
  */
class GBTRegressionOp extends OpNode[SparkBundleContext, GBTRegressionModel, GBTRegressionModel] {
  override val Model: OpModel[SparkBundleContext, GBTRegressionModel] = new OpModel[SparkBundleContext, GBTRegressionModel] {
    override val klazz: Class[GBTRegressionModel] = classOf[GBTRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.gbt_regression

    override def store(model: Model, obj: GBTRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      var i = 0
      val trees = obj.trees.map {
        tree =>
          val name = s"tree$i"
          ModelSerializer(context.bundleContext(name)).write(tree).get
          i = i + 1
          name
      }
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("tree_weights", Value.doubleList(obj.treeWeights)).
        withAttr("trees", Value.stringList(trees))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): GBTRegressionModel = {
      val numFeatures = model.value("num_features").getLong.toInt
      val treeWeights = model.value("tree_weights").getDoubleList.toArray

      val models = model.value("trees").getStringList.map {
        tree => ModelSerializer(context.bundleContext(tree)).read().get.asInstanceOf[DecisionTreeRegressionModel]
      }.toArray

      new GBTRegressionModel(uid = "",
        _trees = models,
        _treeWeights = treeWeights,
        numFeatures = numFeatures)
    }
  }

  override val klazz: Class[GBTRegressionModel] = classOf[GBTRegressionModel]

  override def name(node: GBTRegressionModel): String = node.uid

  override def model(node: GBTRegressionModel): GBTRegressionModel = node

  override def load(node: Node, model: GBTRegressionModel)
                   (implicit context: BundleContext[SparkBundleContext]): GBTRegressionModel = {
    new GBTRegressionModel(uid = node.name,
      _trees = model.trees,
      _treeWeights = model.treeWeights,
      numFeatures = model.numFeatures).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: GBTRegressionModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val dataset = context.context.dataset
    Shape().withInput(node.getFeaturesCol, "features", fieldType(node.getFeaturesCol, dataset))
      .withOutput(node.getPredictionCol, "prediction", fieldType(node.getPredictionCol, dataset))
  }
}
