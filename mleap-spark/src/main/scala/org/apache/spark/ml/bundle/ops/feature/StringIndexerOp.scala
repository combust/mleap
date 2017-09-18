package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import org.apache.avro.generic.GenericData.StringType
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
      val dataset = context.context.dataset.get
      val field = dataset.schema(obj.getInputCol)
      val isNullable = field.dataType == org.apache.spark.sql.types.StringType && field.nullable

      model.withValue("labels", Value.stringList(obj.labels)).
        withValue("nullable_input", Value.boolean(isNullable)).
        withValue("handle_invalid", Value.string(obj.getHandleInvalid))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): StringIndexerModel = {
      new StringIndexerModel(uid = "", labels = model.value("labels").getStringList.toArray).
        setHandleInvalid(model.value("handle_invalid").getString)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: StringIndexerModel): StringIndexerModel = {
    new StringIndexerModel(uid = uid,
      labels = model.labels)
  }

  override def sparkInputs(obj: StringIndexerModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: StringIndexerModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
