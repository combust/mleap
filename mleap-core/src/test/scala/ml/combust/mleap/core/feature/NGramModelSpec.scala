package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{BasicType, ListType, StructField}
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by mikhail on 10/16/16.
  */
class NGramModelSpec extends org.scalatest.funspec.AnyFunSpec{
  describe("ngram model") {
    val ngram = NGramModel(2)

    it("returns an array of ngrams") {
      val feature = "A feature transformer that converts the input array of strings into an array of n-grams.".split(" ")
      val nGramOutput = ngram.apply(feature)

      assert(nGramOutput(0) == "A feature")
      assert(nGramOutput(1) == "feature transformer")
      assert(nGramOutput(2) == "transformer that")
      assert(nGramOutput(3) == "that converts")
      assert(nGramOutput(4) == "converts the")

      assert(nGramOutput.length == 14)
    }

    it("has the right input schema") {
      assert(ngram.inputSchema.fields == Seq(StructField("input", ListType(BasicType.String))))
    }

    it("has the right output schema") {
      assert(ngram.outputSchema.fields == Seq(StructField("output", ListType(BasicType.String))))
    }
  }

}
