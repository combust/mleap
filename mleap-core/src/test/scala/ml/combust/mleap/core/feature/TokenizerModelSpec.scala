package ml.combust.mleap.core.feature

import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class TokenizerModelSpec extends FunSpec {
  describe("#apply") {
    val tokenizer = TokenizerModel()

    assert(tokenizer("hello there dude").sameElements(Array("hello", "there", "dude")))
  }
}
