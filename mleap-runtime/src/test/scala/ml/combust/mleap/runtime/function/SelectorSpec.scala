package ml.combust.mleap.runtime.function

import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hollinwilkins on 10/22/16.
  */
class SelectorSpec extends org.scalatest.funspec.AnyFunSpec {
  describe("#applye") {
    it("creates selectors implicitly") {
      val fieldSelector: Selector = "hey"
      val arraySelector: Selector = Seq("hey", "there")

      assert(fieldSelector == FieldSelector("hey"))
      assert(arraySelector == StructSelector(Seq("hey", "there")))
    }
  }
}
