package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.mleap.TypeConverters._
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import org.apache.spark.ml.param.Param

/**
  * Created by fshabbir on 12/1/16.
  */
class BinarizerOp extends SimpleSparkOp[Binarizer] {
  override val Model: OpModel[SparkBundleContext, Binarizer] = new OpModel[SparkBundleContext, Binarizer] {
    override val klazz: Class[Binarizer] = classOf[Binarizer]

    override def opName: String = Bundle.BuiltinOps.feature.binarizer

    override def store(model: Model, obj: Binarizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val dataset = context.context.dataset.get

      model.withValue("threshold", Value.double(obj.getThreshold)).
        withValue("input_shape", Value.dataShape(sparkToMleapDataShape(dataset.schema(obj.getInputCol), Some(dataset))))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Binarizer = {
      new Binarizer(uid = "").setThreshold(model.value("threshold").getDouble)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: Binarizer): Binarizer = {
    new Binarizer(uid = uid)
  }

  override def sparkInputs(obj: Binarizer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: Binarizer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
