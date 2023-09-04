package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{BasicType, ListType, StructField}
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by mikhail on 10/16/16.
  */
class StopWordsRemoverModelSpec extends org.scalatest.funspec.AnyFunSpec {
  describe("StopWordsRemoverModel"){

    val remover = StopWordsRemoverModel(Array("I", "and", "you"), true)

    it("filters the array of strings of stop words") {
      val removerCaseInsensitive = StopWordsRemoverModel(Array("I", "and", "you"), false)

      assert(remover.apply(Array("I", "use", "MLeap")).sameElements(Array("use", "MLeap")))
      assert(remover.apply(Array("You", "use", "MLeap")).sameElements(Array("You", "use", "MLeap")))
      assert(removerCaseInsensitive.apply(Array("You", "use", "MLeap")).sameElements(Array("use", "MLeap")))
    }

    it("has the right input schema") {
      assert(remover.inputSchema.fields == Seq(StructField("input", ListType(BasicType.String))))
    }

    it("has the right output schema") {
      assert(remover.outputSchema.fields == Seq(StructField("output", ListType(BasicType.String))))
    }
  }

}
