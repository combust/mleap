package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.feature.OneHotEncoderModel

import scala.util.{Failure, Try}

/**
  * Created by hollinwilkins on 8/21/16.
  */
class OneHotEncoderOp extends OpNode[SparkBundleContext, OneHotEncoderModel, OneHotEncoderModel] {
  override val Model: OpModel[SparkBundleContext, OneHotEncoderModel] = new OpModel[SparkBundleContext, OneHotEncoderModel] {
    override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(model: Model, obj: OneHotEncoderModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("size", Value.long(obj.size)).
        withAttr("drop_last", Value.boolean(obj.getDropLast))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): OneHotEncoderModel = {
      new OneHotEncoderModel(uid = "", size = model.value("size").getLong.toInt)
    }
  }

  override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

  override def name(node: OneHotEncoderModel): String = node.uid

  override def model(node: OneHotEncoderModel): OneHotEncoderModel = node

  override def load(node: Node, model: OneHotEncoderModel)
                   (implicit context: BundleContext[SparkBundleContext]): OneHotEncoderModel = {
    new OneHotEncoderModel(uid = node.name, size = model.size).
      setDropLast(model.getDropLast).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: OneHotEncoderModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    Shape().withStandardIO(node.getInputCol, node.getOutputCol)
  }
}
