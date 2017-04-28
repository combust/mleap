package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.sql.mleap.TypeConverters.mleapType
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 8/21/16.
  */
class StringIndexerOp extends OpNode[SparkBundleContext, StringIndexerModel, StringIndexerModel] {
  override val Model: OpModel[SparkBundleContext, StringIndexerModel] = new OpModel[SparkBundleContext, StringIndexerModel] {
    override val klazz: Class[StringIndexerModel] = classOf[StringIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(model: Model, obj: StringIndexerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      context.context.dataset.map(dataset => {
        model.withAttr("input_types", Value.dataType(mleapType(dataset.schema(obj.getInputCol).dataType)))
          .withAttr("output_types", Value.dataType(mleapType(dataset.schema(obj.getOutputCol).dataType)))
      }).getOrElse(model)
        .withAttr("labels", Value.stringList(obj.labels))
        .withAttr("handle_invalid", Value.string(obj.getHandleInvalid))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): StringIndexerModel = {
      new StringIndexerModel(uid = "", labels = model.value("labels").getStringList.toArray).
        setHandleInvalid(model.value("handle_invalid").getString)
    }
  }

  override val klazz: Class[StringIndexerModel] = classOf[StringIndexerModel]

  override def name(node: StringIndexerModel): String = node.uid

  override def model(node: StringIndexerModel): StringIndexerModel = node

  override def load(node: Node, model: StringIndexerModel)
                   (implicit context: BundleContext[SparkBundleContext]): StringIndexerModel = {
    new StringIndexerModel(uid = node.name, labels = model.labels).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: StringIndexerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
