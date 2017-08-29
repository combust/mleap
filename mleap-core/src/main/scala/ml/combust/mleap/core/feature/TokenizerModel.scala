package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{BasicType, ListType, ScalarType, StructType}

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

  override def inputSchema: StructType = StructType("input" -> ScalarType.String).get

  override def outputSchema: StructType = StructType("output" -> ListType(BasicType.String)).get
}
