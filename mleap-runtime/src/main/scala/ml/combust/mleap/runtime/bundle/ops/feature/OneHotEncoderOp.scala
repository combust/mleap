package ml.combust.mleap.runtime.bundle.ops.feature

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.runtime.transformer.feature.OneHotEncoder

/**
  * Created by hollinwilkins on 10/24/16.
  */
object OneHotEncoderOp extends OpNode[OneHotEncoder, OneHotEncoderModel] {
  override val Model: OpModel[OneHotEncoderModel] = new OpModel[OneHotEncoderModel] {
    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(context: BundleContext, model: Model, obj: OneHotEncoderModel): Model = {
      model.withAttr("size", Value.long(obj.size))
    }

    override def load(context: BundleContext, model: Model): OneHotEncoderModel = {
      OneHotEncoderModel(size = model.value("size").getLong.toInt)
    }
  }

  override def name(node: OneHotEncoder): String = node.uid

  override def model(node: OneHotEncoder): OneHotEncoderModel = node.model

  override def load(context: BundleContext, node: Node, model: OneHotEncoderModel): OneHotEncoder = {
    OneHotEncoder(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: OneHotEncoder): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
