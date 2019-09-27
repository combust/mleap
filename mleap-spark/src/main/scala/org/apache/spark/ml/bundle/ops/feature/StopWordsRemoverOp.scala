package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.StopWordsRemover

/**
  * Created by mikhail on 10/16/16.
  */
class StopWordsRemoverOp extends SimpleSparkOp[StopWordsRemover] {
  override val Model: OpModel[SparkBundleContext, StopWordsRemover] = new OpModel[SparkBundleContext, StopWordsRemover] {
    override val klazz: Class[StopWordsRemover] = classOf[StopWordsRemover]

    override def opName: String = Bundle.BuiltinOps.feature.stopwords_remover

    override def store(model: Model, obj: StopWordsRemover)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("stop_words", Value.stringList(obj.getStopWords)).
        withValue("case_sensitive", Value.boolean(obj.getCaseSensitive))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): StopWordsRemover = {
      new StopWordsRemover(uid = "").setStopWords(model.value("stop_words").getStringList.toArray).
        setCaseSensitive(model.value("case_sensitive").getBoolean)
    }

  }

  override def sparkLoad(uid: String, shape: NodeShape, model: StopWordsRemover): StopWordsRemover = {
    new StopWordsRemover(uid = uid).setStopWords(model.getStopWords).setCaseSensitive(model.getCaseSensitive)
  }

  override def sparkInputs(obj: StopWordsRemover): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: StopWordsRemover): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
