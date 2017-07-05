package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.NGram

/**
  * Created by mikhail on 10/16/16.
  */
class NGramOp extends SimpleSparkOp[NGram] {
  override val Model: OpModel[SparkBundleContext, NGram] = new OpModel[SparkBundleContext, NGram] {
    override val klazz: Class[NGram] = classOf[NGram]

    override def opName: String = Bundle.BuiltinOps.feature.ngram

    override def store(model: Model, obj: NGram)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("n", Value.long(obj.getN))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): NGram = {
      new NGram(uid = "").setN(model.value("n").getLong.toInt)
    }

  }

  override def sparkLoad(uid: String, shape: NodeShape, model: NGram): NGram = {
    new NGram(uid = uid)
  }

  override def sparkInputs(obj: NGram): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: NGram): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
