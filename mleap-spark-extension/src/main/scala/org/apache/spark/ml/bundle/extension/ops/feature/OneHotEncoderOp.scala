package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.mleap.feature.OneHotEncoderModel

import scala.util.{Failure, Try}

/**
  * Created by hollinwilkins on 8/21/16.
  */
class OneHotEncoderOp extends SimpleSparkOp[OneHotEncoderModel] {
  override val Model: OpModel[SparkBundleContext, OneHotEncoderModel] = new OpModel[SparkBundleContext, OneHotEncoderModel] {
    override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(model: Model, obj: OneHotEncoderModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("size", Value.long(obj.size)).
        withValue("drop_last", Value.boolean(obj.getDropLast))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): OneHotEncoderModel = {
      new OneHotEncoderModel(uid = "", size = model.value("size").getLong.toInt)
    }
  }



  override def sparkLoad(uid: String, shape: NodeShape, model: OneHotEncoderModel): OneHotEncoderModel = {
    new OneHotEncoderModel(uid = uid, size = model.size)
  }

  override def sparkInputs(obj: OneHotEncoderModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: OneHotEncoderModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
