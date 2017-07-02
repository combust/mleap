package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.StringMapModel
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.feature.StringMap

/**
  * Created by hollinwilkins on 2/5/17.
  */
class StringMapOp extends OpNode[SparkBundleContext, StringMap, StringMapModel] {
  override val Model: OpModel[SparkBundleContext, StringMapModel] = new OpModel[SparkBundleContext, StringMapModel] {
    // the class of the model is needed for when we go to serialize JVM objects
    override val klazz: Class[StringMapModel] = classOf[StringMapModel]

    // a unique name for our op: "string_map"
    // this should be the same as for the MLeap transformer serialization
    override def opName: String = Bundle.BuiltinOps.feature.string_map

    override def store(model: Model, obj: StringMapModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      // unzip our label map so we can store the label and the value
      // as two parallel arrays, we do this because MLeap Bundles do
      // not support storing data as a map
      val (labels, values) = obj.labels.toSeq.unzip

      // add the labels and values to the Bundle model that
      // will be serialized to our MLeap bundle
      model.withValue("labels", Value.stringList(labels)).
        withValue("values", Value.doubleList(values))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): StringMapModel = {
      // retrieve our list of labels
      val labels = model.value("labels").getStringList

      // retrieve our list of values
      val values = model.value("values").getDoubleList

      // reconstruct the model using the parallel labels and values
      StringMapModel(labels.zip(values).toMap)
    }
  }
  override val klazz: Class[StringMap] = classOf[StringMap]

  override def name(node: StringMap): String = node.uid

  override def model(node: StringMap): StringMapModel = node.model

  override def load(node: Node, model: StringMapModel)
                   (implicit context: BundleContext[SparkBundleContext]): StringMap = {
    new StringMap(uid = node.name, model = model).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: StringMap): NodeShape = NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
