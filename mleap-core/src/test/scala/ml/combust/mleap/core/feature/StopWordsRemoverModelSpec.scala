package ml.combust.mleap.core.feature

import org.scalatest.FunSpec

/**
  * Created by mikhail on 10/16/16.
  */
class StopWordsRemoverModelSpec extends FunSpec{
  describe("#apply"){
    it("filters the array of strings of stop words") {
      val remover = StopWordsRemoverModel(Array("I", "and", "you"), true)
      val removerCaseInsensitive = StopWordsRemoverModel(Array("I", "and", "you"), false)

      assert(remover.apply(Array("I", "use", "MLeap")).sameElements(Array("use", "MLeap")))
      assert(remover.apply(Array("You", "use", "MLeap")).sameElements(Array("You", "use", "MLeap")))
      assert(removerCaseInsensitive.apply(Array("You", "use", "MLeap")).sameElements(Array("use", "MLeap")))
    }
  }

}
