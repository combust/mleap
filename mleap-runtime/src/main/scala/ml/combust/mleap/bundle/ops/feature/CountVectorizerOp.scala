package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.CountVectorizerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.CountVectorizer

/**
  * Created by hollinwilkins on 12/28/16.
  */
class CountVectorizerOp extends OpNode[MleapContext, CountVectorizer, CountVectorizerModel] {
  override val Model: OpModel[MleapContext, CountVectorizerModel] = new OpModel[MleapContext, CountVectorizerModel] {
    override val klazz: Class[CountVectorizerModel] = classOf[CountVectorizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.count_vectorizer

    override def store(model: Model, obj: CountVectorizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("vocabulary", Value.stringList(obj.vocabulary)).
        withAttr("binary", Value.boolean(obj.binary)).
        withAttr("min_tf", Value.double(obj.minTf))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): CountVectorizerModel = {
      CountVectorizerModel(vocabulary = model.value("vocabulary").getStringList.toArray,
        binary = model.value("binary").getBoolean,
        minTf = model.value("min_tf").getDouble)
    }
  }

  override val klazz: Class[CountVectorizer] = classOf[CountVectorizer]

  override def name(node: CountVectorizer): String = node.uid

  override def model(node: CountVectorizer): CountVectorizerModel = node.model

  override def load(node: Node, model: CountVectorizerModel)
                   (implicit context: BundleContext[MleapContext]): CountVectorizer = {
    CountVectorizer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: CountVectorizer): NodeShape = NodeShape().withStandardIO(node.inputCol, node.outputCol)
}
