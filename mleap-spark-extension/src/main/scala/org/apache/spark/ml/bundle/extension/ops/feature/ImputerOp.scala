package org.apache.spark.ml.bundle.extension.ops.feature

import ml.bundle.DataType.DataType
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.feature.ImputerModel
import org.apache.spark.sql.mleap.TypeConverters._
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by mikhail on 12/18/16.
  */
class ImputerOp extends OpNode[SparkBundleContext, ImputerModel, ImputerModel] {
  override val Model: OpModel[SparkBundleContext, ImputerModel] = new OpModel[SparkBundleContext, ImputerModel] {
    override val klazz: Class[ImputerModel] = classOf[ImputerModel]

    override def opName: String = Bundle.BuiltinOps.feature.imputer

    override def store(model: Model, obj: ImputerModel)(implicit context: BundleContext[SparkBundleContext]): Model = {
      context.context.dataset.map(dataset => {
        model.withAttr("input_types", Value.dataType(mleapType(dataset.schema(obj.getInputCol).dataType)))
             .withAttr("output_types", Value.dataType(mleapType(dataset.schema(obj.getOutputCol).dataType)))
      }).getOrElse(model)
        .withAttr("surrogate_value", Value.double(obj.surrogateValue))
        .withAttr("missing_value", Value.double(obj.getMissingValue))
        .withAttr("strategy", Value.string(obj.getStrategy))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): ImputerModel = {
      val missingValue = model.value("missing_value").getDouble
      val surrogateValue = model.value("surrogate_value").getDouble
      val strategy = model.value("strategy").getString

      new ImputerModel(uid = "", surrogateValue = surrogateValue).
        setMissingValue(missingValue).
        setStrategy(strategy)
    }
  }

  override val klazz: Class[ImputerModel] = classOf[ImputerModel]

  override def name(node: ImputerModel): String = node.uid

  override def model(node: ImputerModel): ImputerModel = node

  override def load(node: Node, model: ImputerModel)
                   (implicit context: BundleContext[SparkBundleContext]): ImputerModel = {
    new ImputerModel(uid = node.name, surrogateValue = model.surrogateValue).
      setMissingValue(model.getMissingValue).
      setStrategy(model.getStrategy).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: ImputerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)

}
