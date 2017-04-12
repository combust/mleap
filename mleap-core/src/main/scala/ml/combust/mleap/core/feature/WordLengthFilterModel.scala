package ml.combust.mleap.core.feature

/**
  * Created by mageswarand on 14/2/17.
  */

case class WordLengthFilterModel(length: Int = 3) extends Serializable {
  def apply(words: Seq[String]): Seq[String] =  {
    words.filter(word =>
      word.forall(w =>
        w.isLetter) && word.length > length)
  }
}