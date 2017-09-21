package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.types.TensorShape
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.mleap.TypeConverters.sparkToMleapDataShape

/**
  * Created by hollinwilkins on 12/28/16.
  */
class DCTOp extends SimpleSparkOp[DCT] {
  override val Model: OpModel[SparkBundleContext, DCT] = new OpModel[SparkBundleContext, DCT] {
    override val klazz: Class[DCT] = classOf[DCT]

    override def opName: String = Bundle.BuiltinOps.feature.dct

    override def store(model: Model, obj: DCT)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val dataset = context.context.dataset.get
      val inputShape = sparkToMleapDataShape(dataset.schema(obj.getInputCol), dataset).asInstanceOf[TensorShape]

      model.withValue("inverse", Value.boolean(obj.getInverse))
        .withValue("input_size", Value.int(inputShape.dimensions.get.head))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): DCT = {
      new DCT(uid = "").setInverse(model.value("inverse").getBoolean)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: DCT): DCT = {
    new DCT(uid = uid)
  }

  override def sparkInputs(obj: DCT): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: DCT): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
