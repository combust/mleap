package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import org.apache.spark.ml.feature.StopWordsRemover

/**
  * Created by mikhail on 10/16/16.
  */
object StopWordsRemoverOp extends OpNode[StopWordsRemover, StopWordsRemover] {
  override val Model: OpModel[StopWordsRemover] = new OpModel[StopWordsRemover] {
    override def opName: String = Bundle.BuiltinOps.feature.stopwords_remover

    override def store(context: BundleContext, model: Model, obj: StopWordsRemover): Model = {
      model.withAttr("stop_words", Value.stringList(obj.getStopWords)).
        withAttr("case_sensitive", Value.boolean(obj.getCaseSensitive))
    }

    override def load(context: BundleContext, model: Model): StopWordsRemover = {
      new StopWordsRemover(uid = "").setStopWords(model.value("stop_words").getStringVector.toArray).
        setCaseSensitive(model.value("case_sensitive").getBoolean)
    }

  }
  override def name(node: StopWordsRemover): String = node.uid

  override def model(node: StopWordsRemover): StopWordsRemover = node

  override def load(context: BundleContext, node: Node, model: StopWordsRemover): StopWordsRemover = {
    new StopWordsRemover(uid = node.name).
      setStopWords(model.getStopWords).
      setCaseSensitive(model.getCaseSensitive).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: StopWordsRemover): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
