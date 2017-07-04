package ml.combust.mleap.runtime.function

import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/22/16.
  */
class SelectorSpec extends FunSpec {
  describe("#applye") {
    it("creates selectors implicitly") {
      val fieldSelector: Selector = "hey"
      val arraySelector: Selector = Array("hey", "there")

      assert(fieldSelector == FieldSelector("hey"))
      assert(arraySelector == StructSelector("hey", "there"))
    }
  }
}
