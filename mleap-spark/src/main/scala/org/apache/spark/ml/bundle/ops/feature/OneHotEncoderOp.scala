package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.feature.OneHotEncoderModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
class OneHotEncoderOp extends OpNode[SparkBundleContext, OneHotEncoderModel, OneHotEncoderModel] {
  override val Model: OpModel[SparkBundleContext, OneHotEncoderModel] = new OpModel[SparkBundleContext, OneHotEncoderModel] {
    override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: OneHotEncoderModel): Model = {
      model.withAttr("size", Value.long(obj.size))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): OneHotEncoderModel = {
      new OneHotEncoderModel(uid = "", size = model.value("size").getLong.toInt)
    }
  }

  override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

  override def name(node: OneHotEncoderModel): String = node.uid

  override def model(node: OneHotEncoderModel): OneHotEncoderModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: OneHotEncoderModel): OneHotEncoderModel = {
    new OneHotEncoderModel(uid = node.name, size = model.size)
  }

  override def shape(node: OneHotEncoderModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
