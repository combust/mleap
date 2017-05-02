package ml.combust.mleap.runtime.transformer

import org.scalatest.FunSpec

class PipelineSpec extends FunSpec {

  describe("#getSchema") {
    it("has no inputs or outputs itself") {
      val pipeline = new Pipeline(uid = "pipeline", Seq())
      assert(pipeline.getSchema().get == Seq())
    }
  }
}
