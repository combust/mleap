package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.CountVectorizerModel

/**
  * Created by hollinwilkins on 12/28/16.
  */
class CountVectorizerOp extends OpNode[SparkBundleContext, CountVectorizerModel, CountVectorizerModel] {
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

  override val klazz: Class[CountVectorizerModel] = classOf[CountVectorizerModel]

  override def name(node: CountVectorizerModel): String = node.uid

  override def model(node: CountVectorizerModel): CountVectorizerModel = node

  override def load(node: Node, model: CountVectorizerModel)
                   (implicit context: BundleContext[SparkBundleContext]): CountVectorizerModel = {
    new CountVectorizerModel(uid = node.name,
      vocabulary = model.vocabulary).
      setBinary(model.getBinary).
      setMinTF(model.getMinTF).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: CountVectorizerModel): NodeShape = NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
