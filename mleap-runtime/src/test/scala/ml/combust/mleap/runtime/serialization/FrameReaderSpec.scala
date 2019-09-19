package ml.combust.mleap.runtime.serialization

import java.io.File

import org.scalatest.FunSpec

class FrameReaderSpec extends FunSpec {

  describe("frame reader") {
    it("can read frame") {
      val frameFile = new File(getClass.getResource("/adult_census_leapframe.json").toURI)
      val frame = FrameReader().read(frameFile).get
      assert(frame.dataset.size == 1)
    }
  }
}