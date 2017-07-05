package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.Tokenizer

/**
  * Created by hollinwilkins on 8/21/16.
  */
class TokenizerOp extends SimpleSparkOp[Tokenizer] {
  override val Model: OpModel[SparkBundleContext, Tokenizer] = new OpModel[SparkBundleContext, Tokenizer] {
    override val klazz: Class[Tokenizer] = classOf[Tokenizer]

    override def opName: String = Bundle.BuiltinOps.feature.tokenizer

    override def store(model: Model, obj: Tokenizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = { model }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Tokenizer = new Tokenizer(uid = "")
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: Tokenizer): Tokenizer = {
    new Tokenizer(uid = uid)
  }

  override def sparkInputs(obj: Tokenizer): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: Tokenizer): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
