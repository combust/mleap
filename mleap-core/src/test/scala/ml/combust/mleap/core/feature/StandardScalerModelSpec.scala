package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class StandardScalerModelSpec extends FunSpec {
  describe("standard scalar") {
    describe("with mean") {
      val scaler = StandardScalerModel(None, Some(Vectors.dense(Array(50.0, 20.0, 30.0))))

      it("scales based off of the mean") {
         val expectedVector = Array(5.0, 5.0, 3.0)

        assert(scaler(Vectors.dense(Array(55.0, 25.0, 33.0))).toArray.sameElements(expectedVector))
      }

      it("has the right input schema") {
        assert(scaler.inputSchema.fields == Seq(StructField("input", TensorType.Double(3))))
      }

      it("has the right output schema") {
        assert(scaler.outputSchema.fields == Seq(StructField("output", TensorType.Double(3))))
      }
    }

    describe("with stdev") {
      val scaler = StandardScalerModel(Some(Vectors.dense(Array(2.5, 8.0, 10.0))), None)

      it("scales based off the standard deviation") {
         val expectedVector = Array(1.6, .4375, 1.0)

        assert(scaler(Vectors.dense(Array(4.0, 3.5, 10.0))).toArray.sameElements(expectedVector))
      }

      it("has the right input schema") {
        assert(scaler.inputSchema.fields == Seq(StructField("input", TensorType.Double(3))))
      }

      it("has the right output schema") {
        assert(scaler.outputSchema.fields == Seq(StructField("output", TensorType.Double(3))))
      }
    }

    describe("with mean and stdev") {
      val scaler = StandardScalerModel(Some(Vectors.dense(Array(2.5, 8.0, 10.0))),
        Some(Vectors.dense(Array(50.0, 20.0, 30.0))))

      it("scales based off the mean and standard deviation") {
        val expectedVector = Array(1.6, .4375, 1.0)

        assert(scaler(Vectors.dense(Array(54.0, 23.5, 40.0))).toArray.sameElements(expectedVector))
      }

      it("has the right input schema") {
        assert(scaler.inputSchema.fields == Seq(StructField("input", TensorType.Double(3))))
      }

      it("has the right output schema") {
        assert(scaler.outputSchema.fields == Seq(StructField("output", TensorType.Double(3))))
      }
    }
  }
}
