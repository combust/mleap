package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.{WordToVectorKernel, WordToVectorModel}
import ml.combust.mleap.core.types.ListShape
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.WordToVector

import scala.util.Try

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
        withValue("word_vectors", Value.doubleList(obj.wordVectors)).
        withValue("kernel", Value.string(obj.kernel.name))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): WordToVectorModel = {
      val words = model.value("words").getStringList

      // If indices list is not set explicitly, assume words are ordered 0 to n
      val indices = model.getValue("indices").
        map(_.getLongList.map(_.toInt)).
        getOrElse(words.indices)
      val map = words.zip(indices).toMap
      val wv = model.value("word_vectors")
      val wordVectors = Try(wv.getDoubleList.toArray).orElse(Try(wv.getTensor[Double].rawValues)).get
      val kernel = model.getValue("kernel").
        map(_.getString).
        map(WordToVectorKernel.forName).
        getOrElse(WordToVectorKernel.Default)

      WordToVectorModel(wordIndex = map,
        wordVectors = wordVectors,
        kernel = kernel)
    }
  }

  override def model(node: WordToVector): WordToVectorModel = node.model
}
