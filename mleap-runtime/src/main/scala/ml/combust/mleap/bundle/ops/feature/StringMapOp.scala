package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.{HandleInvalid, StringMapModel}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.StringMap

/**
  * Created by hollinwilkins on 1/5/17.
  */
class StringMapOp extends MleapOp[StringMap, StringMapModel] {
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
      model.withValue("labels", Value.stringList(labels))
        .withValue("values", Value.doubleList(values))
        .withValue("handle_invalid", Value.string(obj.handleInvalid.asParamString))
        .withValue("default_value", Value.double(obj.defaultValue))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): StringMapModel = {
      // retrieve our list of labels
      val labels = model.value("labels").getStringList

      // retrieve our list of values
      val values = model.value("values").getDoubleList

      val handleInvalid = model.getValue("handle_invalid").map(_.getString).map(HandleInvalid.fromString(_, false)).getOrElse(HandleInvalid.default)
      val defaultValue = model.getValue("default_value").map(_.getDouble).getOrElse(StringMapModel.defaultValue)

      // reconstruct the model using the parallel labels and values
      StringMapModel(labels.zip(values).toMap, handleInvalid = handleInvalid, defaultValue = defaultValue)
    }
  }

  // the core model that is used by the transformer
  override def model(node: StringMap): StringMapModel = node.model
}
