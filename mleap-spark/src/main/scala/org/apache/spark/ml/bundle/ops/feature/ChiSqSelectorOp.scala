package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.ChiSqSelectorModel
import org.apache.spark.mllib.feature

/**
  * Created by hollinwilkins on 12/27/16.
  */
class ChiSqSelectorOp extends OpNode[SparkBundleContext, ChiSqSelectorModel, ChiSqSelectorModel] {
  override val Model: OpModel[SparkBundleContext, ChiSqSelectorModel] = new OpModel[SparkBundleContext, ChiSqSelectorModel] {
    override val klazz: Class[ChiSqSelectorModel] = classOf[ChiSqSelectorModel]

    override def opName: String = Bundle.BuiltinOps.feature.chi_sq_selector

    override def store(model: Model, obj: ChiSqSelectorModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("filter_indices", Value.longList(obj.selectedFeatures.map(_.toLong).toSeq))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): ChiSqSelectorModel = {
      new ChiSqSelectorModel(uid = "",
        chiSqSelector = new feature.ChiSqSelectorModel(model.value("filter_indices").getLongList.map(_.toInt).toArray))
    }
  }

  override val klazz: Class[ChiSqSelectorModel] = classOf[ChiSqSelectorModel]

  override def name(node: ChiSqSelectorModel): String = node.uid

  override def model(node: ChiSqSelectorModel): ChiSqSelectorModel = node

  override def load(node: Node, model: ChiSqSelectorModel)
                   (implicit context: BundleContext[SparkBundleContext]): ChiSqSelectorModel = {
    new ChiSqSelectorModel(uid = node.name,
      chiSqSelector = new feature.ChiSqSelectorModel(model.selectedFeatures)).
      setFeaturesCol(node.shape.input("features").name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: ChiSqSelectorModel): NodeShape = NodeShape().withInput(node.getFeaturesCol, "features").
    withStandardOutput(node.getOutputCol)
}
