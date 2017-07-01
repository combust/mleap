package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.StopWordsRemover

/**
  * Created by mikhail on 10/16/16.
  */
class StopWordsRemoverOp extends OpNode[SparkBundleContext, StopWordsRemover, StopWordsRemover] {
  override val Model: OpModel[SparkBundleContext, StopWordsRemover] = new OpModel[SparkBundleContext, StopWordsRemover] {
    override val klazz: Class[StopWordsRemover] = classOf[StopWordsRemover]

    override def opName: String = Bundle.BuiltinOps.feature.stopwords_remover

    override def store(model: Model, obj: StopWordsRemover)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("stop_words", Value.stringList(obj.getStopWords)).
        withAttr("case_sensitive", Value.boolean(obj.getCaseSensitive))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): StopWordsRemover = {
      new StopWordsRemover(uid = "").setStopWords(model.value("stop_words").getStringList.toArray).
        setCaseSensitive(model.value("case_sensitive").getBoolean)
    }

  }

  override val klazz: Class[StopWordsRemover] = classOf[StopWordsRemover]

  override def name(node: StopWordsRemover): String = node.uid

  override def model(node: StopWordsRemover): StopWordsRemover = node

  override def load(node: Node, model: StopWordsRemover)
                   (implicit context: BundleContext[SparkBundleContext]): StopWordsRemover = {
    new StopWordsRemover(uid = node.name).
      setStopWords(model.getStopWords).
      setCaseSensitive(model.getCaseSensitive).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: StopWordsRemover): NodeShape = NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
