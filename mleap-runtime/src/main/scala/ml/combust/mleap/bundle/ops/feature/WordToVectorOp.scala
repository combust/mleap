package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.WordToVectorModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.WordToVector

/**
  * Created by hollinwilkins on 12/28/16.
  */
class WordToVectorOp extends MleapOp[WordToVector, WordToVectorModel] {
  override val Model: OpModel[MleapContext, WordToVectorModel] = new OpModel[MleapContext, WordToVectorModel] {
    override val klazz: Class[WordToVectorModel] = classOf[WordToVectorModel]

    override def opName: String = Bundle.BuiltinOps.feature.word_to_vector

    override def store(model: Model, obj: WordToVectorModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val (words, indices) = obj.wordIndex.toSeq.unzip
      model.withValue("words", Value.stringList(words)).
        withValue("indices", Value.longList(indices.map(_.toLong))).
        withValue("word_vectors", Value.doubleList(obj.wordVectors))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): WordToVectorModel = {
      val words = model.value("words").getStringList
      val indices = model.value("indices").getLongList.map(_.toInt)
      val map = words.zip(indices).toMap
      val wordVectors = model.value("word_vectors").getDoubleList.toArray

      WordToVectorModel(wordIndex = map, wordVectors = wordVectors)
    }
  }

  override def model(node: WordToVector): WordToVectorModel = node.model
}
