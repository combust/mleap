package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.mleap.feature.ImputerModel

/**
  * Created by mikhail on 12/18/16.
  */
class ImputerOp extends SimpleSparkOp[ImputerModel] {
  override val Model: OpModel[SparkBundleContext, ImputerModel] = new OpModel[SparkBundleContext, ImputerModel] {
    override val klazz: Class[ImputerModel] = classOf[ImputerModel]

    override def opName: String = Bundle.BuiltinOps.feature.imputer

    override def store(model: Model, obj: ImputerModel)(implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val dataset = context.context.dataset.get

        model.withValue("nullable_input", Value.boolean(dataset.schema(obj.getInputCol).nullable)).
          withValue("surrogate_value", Value.double(obj.surrogateValue)).
          withValue("missing_value", Value.double(obj.getMissingValue)).
          withValue("strategy", Value.string(obj.getStrategy))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): ImputerModel = {
      val missingValue = model.getValue("missing_value")
        .map(missing_value => missing_value.getDouble)
        .getOrElse(Double.NaN)
      val surrogateValue = model.value("surrogate_value").getDouble
      val strategy = model.value("strategy").getString

      new ImputerModel(uid = "", surrogateValue = surrogateValue).
        setMissingValue(missingValue).
        setStrategy(strategy)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: ImputerModel): ImputerModel = {
    new ImputerModel(uid = uid, surrogateValue = model.surrogateValue)
  }

  override def sparkInputs(obj: ImputerModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: ImputerModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
