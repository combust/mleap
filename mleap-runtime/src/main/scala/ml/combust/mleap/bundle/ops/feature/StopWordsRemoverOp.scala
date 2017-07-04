package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.StopWordsRemoverModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.StopWordsRemover

/**
  * Created by mikhail on 10/16/16.
  */
class StopWordsRemoverOp extends MleapOp[StopWordsRemover, StopWordsRemoverModel] {
  override val Model: OpModel[MleapContext, StopWordsRemoverModel] = new OpModel[MleapContext, StopWordsRemoverModel] {
    override val klazz: Class[StopWordsRemoverModel] = classOf[StopWordsRemoverModel]

    override def opName: String = Bundle.BuiltinOps.feature.stopwords_remover

    override def store(model: Model, obj: StopWordsRemoverModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("stop_words", Value.stringList(obj.stopWords)).
        withValue("case_sensitive", Value.boolean(obj.caseSensitive))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): StopWordsRemoverModel = {
      StopWordsRemoverModel(stopWords = model.value("stop_words").getStringList,
        caseSensitive = model.value("case_sensitive").getBoolean)
    }

  }

  override def model(node: StopWordsRemover): StopWordsRemoverModel = node.model
}
