package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.param.Param

/**
  * Created by hollinwilkins on 12/28/16.
  */
class CountVectorizerOp extends SimpleSparkOp[CountVectorizerModel] {
  override val Model: OpModel[SparkBundleContext, CountVectorizerModel] = new OpModel[SparkBundleContext, CountVectorizerModel] {
    override val klazz: Class[CountVectorizerModel] = classOf[CountVectorizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.count_vectorizer

    override def store(model: Model, obj: CountVectorizerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("vocabulary", Value.stringList(obj.vocabulary)).
        withValue("binary", Value.boolean(obj.getBinary)).
        withValue("min_tf", Value.double(obj.getMinTF))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): CountVectorizerModel = {
      new CountVectorizerModel(uid = "",
        vocabulary = model.value("vocabulary").getStringList.toArray).
        setBinary(model.value("binary").getBoolean).
        setMinTF(model.value("min_tf").getDouble)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: CountVectorizerModel): CountVectorizerModel = {
    new CountVectorizerModel(uid = uid,
      vocabulary = model.vocabulary)
      .setBinary(model.getBinary)
      .setMinTF(model.getMinTF)
  }

  override def sparkInputs(obj: CountVectorizerModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: CountVectorizerModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
