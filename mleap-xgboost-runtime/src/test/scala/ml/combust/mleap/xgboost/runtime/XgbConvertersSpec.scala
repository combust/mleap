package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.tensor.{SparseTensor, Tensor}
import ml.combust.mleap.xgboost.runtime.struct.FVecFactory
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.scalatest.funspec.AnyFunSpec
import XgbConverters._

/**
 * Pins the sparse-input semantics: an index absent from a SparseVector or sparse Tensor is missing,
 * and a stored entry is a value. This is what XGBoost 1.7.6 did in training and prediction, and what
 * an xgboost4j-spark that hands the stored indices and values to the DMatrix does. Models trained that
 * way route a gap down the default direction of each split, so reading a gap as 0.0 sends every row
 * of a mostly empty vector down the same trees.
 */
class XgbConvertersSpec extends AnyFunSpec {

  private val gapIndex = 1
  private val sparseVector = new SparseVector(4, Array(0, 2, 3), Array(7.0, 0.0, 9.0))
  private val sparseTensor = SparseTensor[Double](Seq(Seq(0), Seq(2), Seq(3)), Array(7.0, 0.0, 9.0), Seq(4))

  describe("FVec conversion") {
    it("reads an index absent from a SparseVector as missing") {
      val fvec = FVecFactory.fromSparseVector(sparseVector)
      assert(fvec.fvalue(gapIndex) == null)
      assert(fvec.fvalue(0) == 7.0f)
      assert(fvec.fvalue(3) == 9.0f)
    }

    it("reads an index absent from a sparse Tensor as missing") {
      val fvec = FVecFactory.fromSparseTensor(sparseTensor)
      assert(fvec.fvalue(gapIndex) == null)
      assert(fvec.fvalue(0) == 7.0f)
      assert(fvec.fvalue(3) == 9.0f)
    }

    it("keeps an explicitly stored 0.0 as a value") {
      assert(FVecFactory.fromSparseVector(sparseVector).fvalue(2) == 0.0f)
      assert(FVecFactory.fromSparseTensor(sparseTensor).fvalue(2) == 0.0f)
    }

    it("reads a stored 0.0 as missing too when the model treats 0.0 as missing") {
      val fromVector = FVecFactory.fromSparseVector(sparseVector, treatsZeroAsNA = true)
      val fromTensor = FVecFactory.fromSparseTensor(sparseTensor, treatsZeroAsNA = true)

      Seq(fromVector, fromTensor).foreach { fvec =>
        assert(fvec.fvalue(gapIndex) == null)
        assert(fvec.fvalue(2) == null)
        assert(fvec.fvalue(0) == 7.0f)
      }
    }

    it("reads a sparse input differently from its dense equivalent only at the gaps") {
      val sparse = FVecFactory.fromSparseVector(sparseVector)
      val dense = FVecFactory.fromDenseVector(new DenseVector(Array(7.0, 0.0, 0.0, 9.0)))

      assert(sparse.fvalue(gapIndex) == null && dense.fvalue(gapIndex) == 0.0f)
      assert(Seq(0, 2, 3).forall(i => sparse.fvalue(i) == dense.fvalue(i)))
    }

    it("reads an empty sparse input as all missing") {
      val fvec = FVecFactory.fromSparseVector(new SparseVector(4, Array.empty[Int], Array.empty[Double]))
      assert((0 until 4).forall(i => fvec.fvalue(i) == null))
    }
  }

  describe("DMatrix conversion") {
    it("counts only the stored entries of a sparse input when missing is NaN") {
      assert(sparseVector.asXGB().nonMissingNum == 3)
      assert(sparseTensor.asXGB().nonMissingNum == 3)
    }

    it("keeps a dense input fully present when missing is NaN") {
      assert(Tensor.denseVector(Array(7.0, 0.0, 0.0, 9.0)).asXGB().nonMissingNum == 4)
      assert(new DenseVector(Array(7.0, 0.0, 0.0, 9.0)).asXGB().nonMissingNum == 4)
    }

    it("counts zeros as absent when the model was trained with missing=0.0") {
      assert(sparseVector.asXGB(0.0f).nonMissingNum == 2)
      assert(sparseTensor.asXGB(0.0f).nonMissingNum == 2)
    }
  }
}
