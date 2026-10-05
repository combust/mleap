package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.tensor.{SparseTensor, Tensor}
import ml.combust.mleap.xgboost.runtime.struct.FVecFactory
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.scalatest.funspec.AnyFunSpec
import XgbConverters._

/**
 * Pins the sparse-input semantics: xgboost4j-spark densifies a SparseVector both when it builds the
 * training DMatrix (XGBoostEstimator.toXGBLabeledPoint) and in XGBoostModel.transform, so an index
 * absent from the input is an explicit 0.0 feature. Only the model's missing value turns it into an
 * absent one. Dropping those indices instead made MLeap disagree with Spark on rows with gaps.
 */
class XgbConvertersSpec extends AnyFunSpec {

  private val gapIndex = 1
  private val sparseVector = new SparseVector(4, Array(0, 2, 3), Array(7.0, 0.0, 9.0))
  private val sparseTensor = SparseTensor[Double](Seq(Seq(0), Seq(2), Seq(3)), Array(7.0, 0.0, 9.0), Seq(4))

  describe("FVec conversion") {
    it("reads an index absent from a SparseVector as 0.0, not as missing") {
      val fvec = FVecFactory.fromSparseVector(sparseVector)
      assert(fvec.fvalue(gapIndex) == 0.0f)
      assert(fvec.fvalue(0) == 7.0f)
      assert(fvec.fvalue(3) == 9.0f)
    }

    it("reads an index absent from a sparse Tensor as 0.0, not as missing") {
      val fvec = FVecFactory.fromSparseTensor(sparseTensor)
      assert(fvec.fvalue(gapIndex) == 0.0f)
      assert(fvec.fvalue(0) == 7.0f)
      assert(fvec.fvalue(3) == 9.0f)
    }

    it("reads gaps and explicit zeros alike as missing when the model treats 0.0 as missing") {
      val fromVector = FVecFactory.fromSparseVector(sparseVector, treatsZeroAsNA = true)
      val fromTensor = FVecFactory.fromSparseTensor(sparseTensor, treatsZeroAsNA = true)

      Seq(fromVector, fromTensor).foreach { fvec =>
        assert(fvec.fvalue(gapIndex) == null)
        assert(fvec.fvalue(2) == null)
        assert(fvec.fvalue(0) == 7.0f)
      }
    }

    it("gives a sparse input the same values as its dense equivalent") {
      val sparse = FVecFactory.fromSparseVector(sparseVector)
      val dense = FVecFactory.fromDenseVector(new DenseVector(Array(7.0, 0.0, 0.0, 9.0)))

      assert((0 until 4).forall(i => sparse.fvalue(i) == dense.fvalue(i)))
    }
  }

  describe("DMatrix conversion") {
    it("counts every index of a sparse input as present when missing is NaN") {
      assert(sparseVector.asXGB().nonMissingNum == 4)
      assert(sparseTensor.asXGB().nonMissingNum == 4)
      assert(Tensor.denseVector(Array(7.0, 0.0, 0.0, 9.0)).asXGB().nonMissingNum == 4)
    }

    it("counts zeros as absent when the model was trained with missing=0.0") {
      assert(sparseVector.asXGB(0.0f).nonMissingNum == 2)
      assert(sparseTensor.asXGB(0.0f).nonMissingNum == 2)
    }
  }
}
