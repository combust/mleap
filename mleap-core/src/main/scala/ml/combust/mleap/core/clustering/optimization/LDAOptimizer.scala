package ml.combust.mleap.core.clustering.optimization

import breeze.linalg.{Vector, sum, DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV}
import breeze.numerics.{abs, exp}
import breeze.stats.distributions.{Gamma, RandBasis}
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.clustering.LDAUtils

/**
  * Created by mageswarand on 15/2/17.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/clustering/LDAOptimizer.scala")
private[clustering] object OnlineLDAOptimizer {
  /**
    * Uses variational inference to infer the topic distribution `gammad` given the term counts
    * for a document. `termCounts` must contain at least one non-zero entry, otherwise Breeze will
    * throw a BLAS error.
    *
    * An optimization (Lee, Seung: Algorithms for non-negative matrix factorization, NIPS 2001)
    * avoids explicit computation of variational parameter `phi`.
    * @see <a href="http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.31.7566">here</a>
    *
    * @return Returns a tuple of `gammad` - estimate of gamma, the topic distribution, `sstatsd` -
    *         statistics for updating lambda and `ids` - list of termCounts vector indices.
    */
  private[clustering] def variationalTopicInference(
                                                     termCounts: Vector[Double],
                                                     expElogbeta: BDM[Double],
                                                     alpha: breeze.linalg.Vector[Double],
                                                     gammaShape: Double,
                                                     k: Int): (BDV[Double], BDM[Double], List[Int]) = {
    val (ids: List[Int], cts: Array[Double]) = termCounts match {
      case v: BDV[Double] => ((0 until v.size).toList, v.data)
      case v: BSV[Double] => (v.index.toList, v.data)
    }
    // Initialize the variational distribution q(theta|gamma) for the mini-batch
    val gammad: BDV[Double] =
      new Gamma(gammaShape, 1.0 / gammaShape)(RandBasis.mt0).samplesVector(k)                   // K
    val expElogthetad: BDV[Double] = exp(LDAUtils.dirichletExpectation(gammad))  // K
    val expElogbetad = expElogbeta(ids, ::).toDenseMatrix                        // ids * K

    val phiNorm: BDV[Double] = expElogbetad * expElogthetad +:+ 1e-100            // ids
    var meanGammaChange = 1D
    val ctsVector = new BDV[Double](cts)                                         // ids

    // Iterate between gamma and phi until convergence
    while (meanGammaChange > 1e-3) {
      val lastgamma = gammad.copy
      //        K                  K * ids               ids
      gammad := (expElogthetad *:* (expElogbetad.t * (ctsVector /:/ phiNorm))) +:+ alpha
      expElogthetad := exp(LDAUtils.dirichletExpectation(gammad))
      // TODO: Keep more values in log space, and only exponentiate when needed.
      phiNorm := expElogbetad * expElogthetad +:+ 1e-100
      meanGammaChange = sum(abs(gammad - lastgamma)) / k
    }

    val sstatsd = expElogthetad.asDenseMatrix.t * (ctsVector /:/ phiNorm).asDenseMatrix
    (gammad, sstatsd, ids)
  }
}