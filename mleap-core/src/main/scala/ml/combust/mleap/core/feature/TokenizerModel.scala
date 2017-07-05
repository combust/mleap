package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model

/** Companion object for defaults.
  */
object TokenizerModel {
  val defaultTokenizer = TokenizerModel("\\s")
}

/** Class for a tokenizer model.
  *
  * @param regex regular expression used for tokenizing strings
  */
case class TokenizerModel(regex: String = "\\s") extends Model {
  /** Tokenize a document string.
    *
    * Uses regex to split the document into an array.
    *
    * @param document string to tokenize
    * @return array of tokens
    */
  def apply(document: String): Seq[String] = document.toLowerCase.split(regex).toSeq
}
