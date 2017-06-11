package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Node, Shape, _}
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.WordLengthFilterModel
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.feature.WordLengthFilter

/**
  * Created by mageswarand on 14/2/17.
  */

class WordLengthFilterOp extends OpNode[SparkBundleContext, WordLengthFilter, WordLengthFilterModel] {
  override val Model: OpModel[SparkBundleContext, WordLengthFilterModel] = new OpModel[SparkBundleContext, WordLengthFilterModel]  {
    override val klazz: Class[WordLengthFilterModel] = classOf[WordLengthFilterModel]

    override def opName: String = Bundle.BuiltinOps.feature.word_filter

    override def store(model: Model, obj: WordLengthFilterModel)(implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("length", Value.int(obj.length))
    }

    override def load(model: Model)(implicit context: BundleContext[SparkBundleContext]): WordLengthFilterModel = {
      new WordLengthFilterModel(model.value("length").getInt)
    }
  }
  override val klazz: Class[WordLengthFilter] = classOf[WordLengthFilter]

  override def name(node: WordLengthFilter): String = node.uid

  override def model(node: WordLengthFilter): WordLengthFilterModel = node.model

  override def shape(node: WordLengthFilter): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)

  override def load(node: Node, model: WordLengthFilterModel)(implicit context: BundleContext[SparkBundleContext]): WordLengthFilter = {
    new WordLengthFilter(uid = node.name).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name).setWordLength(model.length)
  }
}