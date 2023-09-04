package ml.combust.mleap.runtime.transformer.feature

import ml.combust.bundle.BundleFile
import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.transformer.Pipeline

import java.io.File
import scala.util.Using

class OneHotEncoderSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs in a single-input/output context") {
      val transformer = OneHotEncoder(
        shape = NodeShape.feature("input0", "output0", "input0", "output0"),
        model = OneHotEncoderModel(Array(5)))
      assert(
        transformer.schema.fields ==
          Seq(StructField("input0", ScalarType.Double.nonNullable),
            StructField("output0", TensorType.Double(5))))
    }

    it("has the correct inputs and output in a multi-input/output context") {
      val shp = NodeShape()
        .withInput("input0", "input0")
        .withInput("input1", "input1")
        .withOutput("output0", "output0")
        .withOutput("output1", "output1")
      val transformer = OneHotEncoder(shape = shp, model = OneHotEncoderModel(Array(5)))
      assert(transformer.schema.fields ==
        Seq(StructField("input0", ScalarType.Double.nonNullable),
          StructField("output0", TensorType.Double(5))))
    }
  }

  describe("one hot encoder pre Spark 2.3.0") {
    it("loads correctly in mleap") {
      val file = new File(getClass.getResource("/one_hot_encoder_pipeline.zip").toURI)
      val pipeline = Using(BundleFile(file)) { bf =>
        bf.loadMleapBundle().get.root
      }.get.asInstanceOf[Pipeline]

      assert(pipeline.model.transformers.size == 2)
    }
  }
}
