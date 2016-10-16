package ml.combust.mleap.core.feature

import org.scalatest.FunSpec

/**
  * Created by mikhail on 10/16/16.
  */
class NGramModelSpec extends FunSpec{
  describe("#apply") {
    it("returns an array of ngrams") {
      val ngram = NGramModel(2)

      val feature = "A feature transformer that converts the input array of strings into an array of n-grams.".split(" ")

      val nGramOutut = ngram.apply(feature)

      assert(nGramOutut(0) == "A feature")
      assert(nGramOutut(1) == "feature transformer")
      assert(nGramOutut(2) == "transformer that")
      assert(nGramOutut(3) == "that converts")
      assert(nGramOutut(4) == "converts the")

      assert(nGramOutut.length == 14)

    }
  }

}
