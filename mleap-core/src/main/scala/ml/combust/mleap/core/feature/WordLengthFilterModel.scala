package ml.combust.mleap.core.feature

/**
  * Created by mageswarand on 14/2/17.
  *
  * https://github.com/combust/mleap/blob/master/mleap-core/src/main/scala/ml/combust/mleap/core/feature/TokenizerModel.scala
  */

case class WordLengthFilterModel(length: Int = 3) extends Serializable {
  def apply(words: Seq[String]): Seq[String] =  {
    words.filter(word =>
    word.forall(w =>
      w.isLetter) && word.length > length)
  }
}

