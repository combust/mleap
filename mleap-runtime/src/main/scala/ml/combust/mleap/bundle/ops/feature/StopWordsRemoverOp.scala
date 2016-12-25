package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.StopWordsRemoverModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.StopWordsRemover

/**
  * Created by mikhail on 10/16/16.
  */
class StopWordsRemoverOp extends OpNode[MleapContext, StopWordsRemover, StopWordsRemoverModel] {
  override val Model: OpModel[MleapContext, StopWordsRemoverModel] = new OpModel[MleapContext, StopWordsRemoverModel] {
    override val klazz: Class[StopWordsRemoverModel] = classOf[StopWordsRemoverModel]

    override def opName: String = Bundle.BuiltinOps.feature.stopwords_remover

    override def store(model: Model, obj: StopWordsRemoverModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("stop_words", Value.stringList(obj.stopWords)).
        withAttr("case_sensitive", Value.boolean(obj.caseSensitive))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): StopWordsRemoverModel = {
      StopWordsRemoverModel(stopWords = model.value("stop_words").getStringList,
        caseSensitive = model.value("case_sensitive").getBoolean)
    }

  }

  override val klazz: Class[StopWordsRemover] = classOf[StopWordsRemover]

  override def name(node: StopWordsRemover): String = node.uid

  override def model(node: StopWordsRemover): StopWordsRemoverModel = node.model

  override def load(node: Node, model: StopWordsRemoverModel)
                   (implicit context: BundleContext[MleapContext]): StopWordsRemover = {
    StopWordsRemover(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model
    )
  }

  override def shape(node: StopWordsRemover): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
