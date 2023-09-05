package ml.combust.mleap.core.tree

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hwilkins on 1/20/16.
  */
class CategoricalSplitSpec extends org.scalatest.funspec.AnyFunSpec {
  describe("#shouldGoLeft") {
    describe("with features") {
      describe("when isLeft is true") {
        describe("when the feature is in the list") {
          it("returns true") {
            val split = CategoricalSplit(1, 5, Array(1.0, 2.0), isLeft = true)
            assert(split.shouldGoLeft(Vectors.dense(Array(0.0, 2.0))))
          }
        }

        describe("when the feature is not in the list") {
          it("returns false") {
            val split = CategoricalSplit(1, 5, Array(1.0, 2.0), isLeft = true)
            assert(!split.shouldGoLeft(Vectors.dense(Array(0.0, 3.0))))
          }
        }
      }

      describe("when isLeft is false") {
        describe("when the feature is in the list") {
          it("returns false") {
            val split = CategoricalSplit(1, 5, Array(1.0, 2.0), isLeft = false)
            assert(!split.shouldGoLeft(Vectors.dense(Array(0.0, 1.0))))
          }
        }

        describe("when the feature is not in the list") {
          it("returns true") {
            val split = CategoricalSplit(1, 5, Array(1.0, 2.0), isLeft = false)
            assert(split.shouldGoLeft(Vectors.dense(Array(0.0, 3.0))))
          }
        }
      }
    }

    describe("with binned features") {
      describe("when isLeft is true") {
        describe("when the feature is in the list") {
          it("returns true") {
            val split = CategoricalSplit(1, 5, Array(1.0, 2.0), isLeft = true)
            assert(split.shouldGoLeft(1, Array()))
          }
        }

        describe("when the feature is not in the list") {
          it("returns false") {
            val split = CategoricalSplit(1, 5, Array(1.0, 2.0), isLeft = true)
            assert(!split.shouldGoLeft(3, Array()))
          }
        }
      }

      describe("when isLeft is false") {
        describe("when the feature is in the list") {
          it("returns false") {
            val split = CategoricalSplit(1, 5, Array(1.0, 2.0), isLeft = false)
            assert(!split.shouldGoLeft(2, Array()))
          }
        }

        describe("when the feature is not in the list") {
          it("returns true") {
            val split = CategoricalSplit(1, 5, Array(1.0, 2.0), isLeft = false)
            assert(split.shouldGoLeft(3, Array()))
          }
        }
      }
    }
  }
}

class ContinuousSplitSpec extends org.scalatest.funspec.AnyFunSpec {
  describe("#shouldGoLeft") {
    describe("with features") {
      describe("when below threshold") {
        it("returns true") {
          val split = ContinuousSplit(0, 0.4)
          assert(split.shouldGoLeft(Vectors.dense(Array(0.2))))
        }
      }

      describe("when above threshold") {
        it("returns false") {
          val split = ContinuousSplit(0, 0.4)
          assert(!split.shouldGoLeft(Vectors.dense(Array(0.6))))
        }
      }
    }

    describe("with binned features") {
      describe("when binnedFeature == splits.length") {
        it("returns false") {
          val split = ContinuousSplit(1, 0.4)
          assert(!split.shouldGoLeft(1, Array(ContinuousSplit(1, 0.5))))
        }
      }

      describe("when featureUpperBound is <= threshold") {
        it("returns true") {
          val split = ContinuousSplit(1, 0.4)
          assert(split.shouldGoLeft(0, Array(ContinuousSplit(1, 0.3))))
        }
      }

      describe("when featureUpperBound is > threshold") {
        it("returns false") {
          val split = ContinuousSplit(1, 0.4)
          assert(!split.shouldGoLeft(0, Array(ContinuousSplit(1, 0.6))))
        }
      }
    }
  }
}
