package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.StringIndexerModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
object StringIndexerOp extends OpNode[SparkBundleContext, StringIndexerModel, StringIndexerModel] {
  override val Model: OpModel[SparkBundleContext, StringIndexerModel] = new OpModel[SparkBundleContext, StringIndexerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: StringIndexerModel): Model = {
      model.withAttr("labels", Value.stringList(obj.labels))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): StringIndexerModel = {
      new StringIndexerModel(uid = "", labels = model.value("labels").getStringList.toArray)
    }
  }

  override def name(node: StringIndexerModel): String = node.uid

  override def model(node: StringIndexerModel): StringIndexerModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: StringIndexerModel): StringIndexerModel = {
    new StringIndexerModel(uid = node.name, labels = model.labels).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: StringIndexerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
