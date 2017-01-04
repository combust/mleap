package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.IndexToString

/**
  * Created by hollinwilkins on 8/21/16.
  */
class ReverseStringIndexerOp extends OpNode[SparkBundleContext, IndexToString, IndexToString] {
  override val Model: OpModel[SparkBundleContext, IndexToString] = new OpModel[SparkBundleContext, IndexToString] {
    override val klazz: Class[IndexToString] = classOf[IndexToString]

    override def opName: String = Bundle.BuiltinOps.feature.reverse_string_indexer

    override def store(model: Model, obj: IndexToString)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("labels", Value.stringList(obj.getLabels))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): IndexToString = {
      new IndexToString(uid = "").setLabels(model.value("labels").getStringList.toArray)
    }
  }

  override val klazz: Class[IndexToString] = classOf[IndexToString]

  override def name(node: IndexToString): String = node.uid

  override def model(node: IndexToString): IndexToString = node

  override def load(node: Node, model: IndexToString)
                   (implicit context: BundleContext[SparkBundleContext]): IndexToString = {
    new IndexToString(uid = node.name).setLabels(model.getLabels)
  }

  override def shape(node: IndexToString): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
