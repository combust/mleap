package ml.combust.mleap.core.feature

/** Companion object for defaults.
  */
object TokenizerModel {
  val defaultTokenizer = TokenizerModel("\\s")
}

/** Class for a tokenizer model.
  *
  * Default regular expression for tokenizing strings is defined by
  * [[TokenizerModel.defaultTokenizer]]
  *
  * @param regex regular expression used for tokenizing strings
  */
case class TokenizerModel(regex: String = "\\s") {
  /** Tokenize a document string.
    *
    * Uses regex to split the document into an array.
    *
    * @param document string to tokenize
    * @return array of tokens
    */
  def apply(document: String): Array[String] = document.toLowerCase.split(regex)
}
