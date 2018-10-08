package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.StringIndexerModel

/**
  * Created by hollinwilkins on 8/21/16.
  */
class StringIndexerOp extends SimpleSparkOp[StringIndexerModel] {
  override val Model: OpModel[SparkBundleContext, StringIndexerModel] = new OpModel[SparkBundleContext, StringIndexerModel] {
    override val klazz: Class[StringIndexerModel] = classOf[StringIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(model: Model, obj: StringIndexerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("labels", Value.stringList(obj.labels)).
        withValue("handle_invalid", Value.string(obj.getHandleInvalid)).
        withValue("string_order_type", Value.string(obj.getStringOrderType))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): StringIndexerModel = {
      val m = new StringIndexerModel(uid = "", labels = model.value("labels").getStringList.toArray).
        setHandleInvalid(model.value("handle_invalid").getString)
      m.set(m.stringOrderType, model.value("string_order_type").getString)
      m
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: StringIndexerModel): StringIndexerModel = {
    val m = new StringIndexerModel(uid = uid,
      labels = model.labels).setHandleInvalid(model.getHandleInvalid)
    m.set(m.stringOrderType, model.getStringOrderType)
    m
  }

  override def sparkInputs(obj: StringIndexerModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: StringIndexerModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
