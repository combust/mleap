package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Node, Shape, _}
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.WordLengthFilterModel
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.feature.WordFilter
import org.apache.spark.sql.mleap.TypeConverters.fieldType

/**
  * Created by mageswarand on 14/2/17.
  */

class WordLengthFilterOp extends OpNode[SparkBundleContext, WordFilter, WordLengthFilterModel] {
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
  override val klazz: Class[WordFilter] = classOf[WordFilter]

  override def name(node: WordFilter): String = node.uid

  override def model(node: WordFilter): WordLengthFilterModel = node.model

  override def shape(node: WordFilter)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val dataset = context.context.dataset
    Shape().withStandardIO(node.getInputCol, fieldType(node.getInputCol, dataset),
      node.getOutputCol, fieldType(node.getOutputCol, dataset))
  }

  override def load(node: Node, model: WordLengthFilterModel)(implicit context: BundleContext[SparkBundleContext]): WordFilter = {
    new WordFilter(uid = node.name, model = model).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }
}