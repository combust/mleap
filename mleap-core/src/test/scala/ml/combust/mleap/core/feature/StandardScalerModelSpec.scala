package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class StandardScalerModelSpec extends org.scalatest.funspec.AnyFunSpec {
  describe("standard scaler with dense data") {
    describe("with mean") {
      val scaler = StandardScalerModel(None, Some(Vectors.dense(Array(50.0, 20.0, 30.0))))

      it("scales based off of the mean") {
        val expectedVector = Array(5.0, 5.0, 3.0)
        assert(scaler(Vectors.dense(Array(55.0, 25.0, 33.0))).toArray.sameElements(expectedVector))
      }

      it should behave like aModelWithSchema(scaler, 3)
    }

    describe("with stdev") {
      val scaler = StandardScalerModel(Some(Vectors.dense(Array(2.5, 8.0, 10.0))), None)

      it("scales based off the standard deviation") {
        val expectedVector = Array(1.6, .4375, 1.0)
        assert(scaler(Vectors.dense(Array(4.0, 3.5, 10.0))).toArray.sameElements(expectedVector))
      }

      it should behave like aModelWithSchema(scaler, 3)
    }

    describe("with mean and stdev") {
      val scaler = StandardScalerModel(Some(Vectors.dense(Array(2.5, 8.0, 10.0))), Some(Vectors.dense(Array(50.0, 20.0, 30.0))))

      it("scales based off the mean and standard deviation") {
        val expectedVector = Array(1.6, .4375, 1.0)
        assert(scaler(Vectors.dense(Array(54.0, 23.5, 40.0))).toArray.sameElements(expectedVector))
      }

      it should behave like aModelWithSchema(scaler, 3)
    }
  }

  describe("standard scaler with sparse data") {
    describe("with mean") {
      val scaler = StandardScalerModel(None, Some(Vectors.sparse(5, Array(1, 2, 4), Array(20, 45, 100))))

      it("scales based off of the mean") {
        val expectedVector = Array(0.0, 5.0, 5.0, 0.0, 3.0)
        assert(scaler(Vectors.sparse(5, Array(1, 2, 4), Array(25, 50, 103))).toArray.sameElements(expectedVector))
      }

      it should behave like aModelWithSchema(scaler, 5)
    }

    describe("with stdev") {
      val scaler = StandardScalerModel(Some(Vectors.sparse(5, Array(1, 2, 4), Array(20, 45, 100))), None)

      it("scales based off the standard deviation") {
        val expectedVector = Array(0.0, 1.25, 2.2, 0.0, 1.02)
        assert(scaler(Vectors.sparse(5, Array(1, 2, 4), Array(25, 99, 102))).toArray.sameElements(expectedVector))
      }

      it should behave like aModelWithSchema(scaler, 5)
    }

    describe("with mean and stdev") {
      val scaler = StandardScalerModel(Some(Vectors.sparse(5, Array(1, 2, 4), Array(2.5, 8.0, 10.0))),
        Some(Vectors.sparse(5, Array(1, 2, 4), Array(50.0, 20.0, 30.0))))

      it("scales based off the mean and standard deviation") {
        val expectedVector = Array(0.0, 1.6, .4375,  0.0, 1.0)
        val actual = scaler(Vectors.sparse(5, Array(1, 2, 4), Array(54.0, 23.5, 40.0)))
        assert(actual.toArray.sameElements(expectedVector))
      }

      it should behave like aModelWithSchema(scaler, 5)
    }
  }

  def aModelWithSchema(model: StandardScalerModel, tensorSize: Integer) = {
    it("has the right input schema") {
      assert(model.inputSchema.fields == Seq(StructField("input", TensorType.Double(tensorSize))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields == Seq(StructField("output", TensorType.Double(tensorSize))))
    }
  }
}
