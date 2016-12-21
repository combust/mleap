package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.feature.{ImputerModel, Imputer}

/**
  * Created by mikhail on 12/18/16.
  */
class ImputerOp extends OpNode[SparkBundleContext, ImputerModel, ImputerModel] {
  override val Model: OpModel[SparkBundleContext, ImputerModel] = new OpModel[SparkBundleContext, ImputerModel] {
    override val klazz: Class[ImputerModel] = classOf[ImputerModel]

    override def opName: String = Bundle.BuiltinOps.feature.imputer

    override def store(model: Model, obj: ImputerModel)(implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("imputeValue", Value.double(obj.imputeValue))
        .withAttr("missingValue", Value.double(obj.getMissingValue))
        .withAttr("strategy", Value.string(obj.getStrategy))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Imputer = {
      val missingValue = model.value("missingValue").getDouble

      val imputeValue = model.value("imputeValue").getDouble

      new ImputerModel(uid = "", missingValue = missingValue, imputeValue = imputeValue)

      new Imputer(uid = "").
        setStrategy(model.value("strategy").getString).
        setMissingValue(model.value("missingValue").getDouble)
    }
  }

  override val klazz: Class[ImputerModel] = classOf[ImputerModel]

  override def name(node: ImputerModel): String = node.uid

  override def model(node: ImputerModel): ImputerModel = node

  override def load(node: Node, model: ImputerModel)
                   (implicit context: BundleContext[SparkBundleContext]): ImputerModel = {
    new ImputerModel(uid = node.name, imputeValue = model.imputeValue, missingValue = model.missingValue)
  }

  override def shape(node: ImputerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)

}
