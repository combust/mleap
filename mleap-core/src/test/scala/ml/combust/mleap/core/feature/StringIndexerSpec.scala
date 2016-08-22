package ml.combust.mleap.core.feature

import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class StringIndexerSpec extends FunSpec {
  describe("#apply") {
    it("returns the index of the string") {
      val indexer = StringIndexerModel(Array("hello", "there", "dude"))

      assert(indexer("hello") == 0.0)
      assert(indexer("there") == 1.0)
      assert(indexer("dude") == 2.0)
    }
  }
}
