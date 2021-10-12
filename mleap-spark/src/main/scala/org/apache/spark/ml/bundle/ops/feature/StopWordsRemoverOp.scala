package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel}
import org.apache.spark.ml.bundle.{MultiInOutFormatSparkOp, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.StopWordsRemover

/**
  * Created by mikhail on 10/16/16.
  */
class StopWordsRemoverOp extends SimpleSparkOp[StopWordsRemover] with MultiInOutFormatSparkOp[StopWordsRemover] {
  override val Model: OpModel[SparkBundleContext, StopWordsRemover] = new OpModel[SparkBundleContext, StopWordsRemover] {
    override val klazz: Class[StopWordsRemover] = classOf[StopWordsRemover]

    override def opName: String = Bundle.BuiltinOps.feature.stopwords_remover

    override def store(model: Model, obj: StopWordsRemover)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val result = model.withValue("stop_words", Value.stringList(obj.getStopWords)).
        withValue("case_sensitive", Value.boolean(obj.getCaseSensitive))
      saveMultiInOutFormat(result, obj)
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): StopWordsRemover = {
      val obj = new StopWordsRemover(uid = "").setStopWords(model.value("stop_words").getStringList.toArray).
        setCaseSensitive(model.value("case_sensitive").getBoolean)
      loadMultiInOutFormat(model, obj)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: StopWordsRemover): StopWordsRemover = {
    val m = new StopWordsRemover(uid)
    m.copy(model.extractParamMap())
    m
  }
}
