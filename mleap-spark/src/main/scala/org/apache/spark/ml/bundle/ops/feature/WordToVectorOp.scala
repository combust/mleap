package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.mllib.feature

/**
  * Created by hollinwilkins on 12/28/16.
  */
class WordToVectorOp extends SimpleSparkOp[Word2VecModel] {
  override val Model: OpModel[SparkBundleContext, Word2VecModel] = new OpModel[SparkBundleContext, Word2VecModel] {
    override val klazz: Class[Word2VecModel] = classOf[Word2VecModel]

    override def opName: String = Bundle.BuiltinOps.feature.word_to_vector

    override def store(model: Model, obj: Word2VecModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val wv = getWordVectors(obj)
      val (words, indices) = wv.wordIndex.toSeq.unzip
      model.withValue("words", Value.stringList(words)).
        withValue("indices", Value.longList(indices.map(_.toLong))).
        withValue("word_vectors", Value.doubleList(wv.wordVectors.map(_.toDouble)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Word2VecModel = {
      val words = model.value("words").getStringList
      val indices = model.value("indices").getLongList.map(_.toInt)
      val map = words.zip(indices).toMap
      val wordVectors = model.value("word_vectors").getDoubleList.toArray

      val wv = new feature.Word2VecModel(map, wordVectors.map(_.toFloat))
      val m = new Word2VecModel(uid = "", wordVectors = wv)
      m.set(m.vectorSize, wordVectors.size / indices.length)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: Word2VecModel): Word2VecModel = {
    val m = new Word2VecModel(uid = uid, wordVectors = getWordVectors(model))
    m.set(m.vectorSize, model.getVectorSize)
  }

  override def sparkInputs(obj: Word2VecModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: Word2VecModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }

  private def getWordVectors(obj: Word2VecModel): feature.Word2VecModel = {
    // UGLY: have to use reflection to get this private field :(
    val wvField = obj.getClass.getDeclaredField("org$apache$spark$ml$feature$Word2VecModel$$wordVectors")
    wvField.setAccessible(true)
    wvField.get(obj).asInstanceOf[feature.Word2VecModel]
  }
}
