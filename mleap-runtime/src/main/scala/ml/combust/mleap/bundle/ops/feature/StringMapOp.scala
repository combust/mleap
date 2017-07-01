package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.StringMapModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.StringMap

/**
  * Created by hollinwilkins on 1/5/17.
  */
class StringMapOp extends OpNode[MleapContext, StringMap, StringMapModel] {
  override val Model: OpModel[MleapContext, StringMapModel] = new OpModel[MleapContext, StringMapModel] {
    // the class of the model is needed for when we go to serialize JVM objects
    override val klazz: Class[StringMapModel] = classOf[StringMapModel]

    // a unique name for our op: "string_map"
    override def opName: String = Bundle.BuiltinOps.feature.string_map

    override def store(model: Model, obj: StringMapModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      // unzip our label map so we can store the label and the value
      // as two parallel arrays, we do this because MLeap Bundles do
      // not support storing data as a map
      val (labels, values) = obj.labels.toSeq.unzip

      // add the labels and values to the Bundle model that
      // will be serialized to our MLeap bundle
      model.withAttr("labels", Value.stringList(labels)).
        withAttr("values", Value.doubleList(values))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): StringMapModel = {
      // retrieve our list of labels
      val labels = model.value("labels").getStringList

      // retrieve our list of values
      val values = model.value("values").getDoubleList

      // reconstruct the model using the parallel labels and values
      StringMapModel(labels.zip(values).toMap)
    }
  }

  // class of the transformer
  override val klazz: Class[StringMap] = classOf[StringMap]

  // unique name in the pipeline for this transformer
  override def name(node: StringMap): String = node.uid

  // the core model that is used by the transformer
  override def model(node: StringMap): StringMapModel = node.model

  // reconstruct our MLeap transformer from the
  // deserialized core model, unique name of this node,
  // and the inputs/outputs of the node
  override def load(node: Node, model: StringMapModel)
                   (implicit context: BundleContext[MleapContext]): StringMap = {
    StringMap(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  // the shape defines the inputs and outputs of our node
  // in this case, we have 1 input and 1 output that
  // are connected to the standard input and output ports for
  // a node. shapes can get fairly complicated and may be confusing at first
  // but all they do is connect fields from a data frame to certain input/output
  // locations of the node itself
  override def shape(node: StringMap): NodeShape = NodeShape().withStandardIO(node.inputCol, node.outputCol)
}
