package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.mllib.feature

/**
  * Created by hollinwilkins on 12/28/16.
  */
class WordToVectorOp extends OpNode[SparkBundleContext, Word2VecModel, Word2VecModel] {
  override val Model: OpModel[SparkBundleContext, Word2VecModel] = new OpModel[SparkBundleContext, Word2VecModel] {
    override val klazz: Class[Word2VecModel] = classOf[Word2VecModel]

    override def opName: String = Bundle.BuiltinOps.feature.word_to_vector

    override def store(model: Model, obj: Word2VecModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val wv = getWordVectors(obj)
      val (words, indices) = wv.wordIndex.toSeq.unzip
      model.withAttr("words", Value.stringList(words)).
        withAttr("indices", Value.longList(indices.map(_.toLong))).
        withAttr("word_vectors", Value.doubleList(wv.wordVectors.map(_.toDouble)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Word2VecModel = {
      val words = model.value("words").getStringList
      val indices = model.value("indices").getLongList.map(_.toInt)
      val map = words.zip(indices).toMap
      val wordVectors = model.value("word_vectors").getDoubleList.toArray

      val wv = new feature.Word2VecModel(map, wordVectors.map(_.toFloat))
      new Word2VecModel(uid = "", wordVectors = wv)
    }
  }

  override val klazz: Class[Word2VecModel] = classOf[Word2VecModel]

  override def name(node: Word2VecModel): String = node.uid

  override def model(node: Word2VecModel): Word2VecModel = node

  override def load(node: Node, model: Word2VecModel)
                   (implicit context: BundleContext[SparkBundleContext]): Word2VecModel = {
    new Word2VecModel(uid = node.name, wordVectors = getWordVectors(model)).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: Word2VecModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    Shape().withStandardIO(node.getInputCol, node.getOutputCol)
  }

  private def getWordVectors(obj: Word2VecModel): feature.Word2VecModel = {
    // UGLY: have to use reflection to get this private field :(
    val wvField = obj.getClass.getDeclaredField("org$apache$spark$ml$feature$Word2VecModel$$wordVectors")
    wvField.setAccessible(true)
    wvField.get(obj).asInstanceOf[feature.Word2VecModel]
  }
}
