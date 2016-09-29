package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.mleap.feature.OneHotEncoderModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
object OneHotEncoderOp extends OpNode[OneHotEncoderModel, OneHotEncoderModel] {
  override val Model: OpModel[OneHotEncoderModel] = new OpModel[OneHotEncoderModel] {
    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(context: BundleContext, model: WritableModel, obj: OneHotEncoderModel): WritableModel = {
      model.withAttr(Attribute("size", Value.long(obj.size)))
    }

    override def load(context: BundleContext, model: ReadableModel): OneHotEncoderModel = {
      new OneHotEncoderModel(uid = "", size = model.value("size").getLong.toInt)
    }
  }

  override def name(node: OneHotEncoderModel): String = node.uid

  override def model(node: OneHotEncoderModel): OneHotEncoderModel = node

  override def load(context: BundleContext, node: ReadableNode, model: OneHotEncoderModel): OneHotEncoderModel = {
    new OneHotEncoderModel(uid = node.name, size = model.size)
  }

  override def shape(node: OneHotEncoderModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
