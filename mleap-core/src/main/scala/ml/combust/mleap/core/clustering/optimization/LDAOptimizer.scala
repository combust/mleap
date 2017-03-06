package ml.combust.mleap.core.clustering.optimization

import java.util.Random

import breeze.linalg.{all, normalize, sum, DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Matrix }
import breeze.numerics.{abs, exp, trigamma}
import breeze.stats.distributions.{Gamma, RandBasis}
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.graphx._
import org.apache.spark.mllib.impl.PeriodicGraphCheckpointer
//import org.apache.spark.mllib.linalg.{DenseVector, Matrices, SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.clustering.{LDA, LDAModel, LDAUtils, LocalLDAModel}

/**
  * Created by mageswarand on 15/2/17.
  */

@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/mllib/clustering/LDAOptimizer.scala")
@DeveloperApi
trait LDAOptimizer {
  private[clustering] def initialize(docs: Array[(Long, BDV[Double])], lda: LDA): LDAOptimizer

  private[clustering] def next(): LDAOptimizer

  private[clustering] def getLDAModel(iterationTimes: Array[Double]): LDAModel
}


@DeveloperApi
final class OnlineLDAOptimizer extends LDAOptimizer {

  // LDA common parameters
  private var k: Int = 0
  private var corpusSize: Long = 0
  private var vocabSize: Int = 0

  /** alias for docConcentration */
  private var alpha: BDV[Double] = BDV.zeros(0)

  /** (for debugging)  Get docConcentration */
  private[clustering] def getAlpha: BDV[Double] = alpha

  /** alias for topicConcentration */
  private var eta: Double = 0

  /** (for debugging)  Get topicConcentration */
  private[clustering] def getEta: Double = eta

  private var randomGenerator: java.util.Random = null

  /** (for debugging) Whether to sample mini-batches with replacement. (default = true) */
  private var sampleWithReplacement: Boolean = true

  // Online LDA specific parameters
  // Learning rate is: (tau0 + t)^{-kappa}
  private var tau0: Double = 1024
  private var kappa: Double = 0.51
  private var miniBatchFraction: Double = 0.05
  private var optimizeDocConcentration: Boolean = false

  // internal data structure
  private var docs: Array[(Long, BDV[Double])] = null //TODO Vector is triat. Here BDV ok??? Int due to count

  /** Dirichlet parameter for the posterior over topics */
  private var lambda: BDM[Double] = null

  /** (for debugging) Get parameter for topics */
  private[clustering] def getLambda: BDM[Double] = lambda

  /** Current iteration (count of invocations of [[next()]]) */
  private var iteration: Int = 0
  private var gammaShape: Double = 100

  /**
    * A (positive) learning parameter that downweights early iterations. Larger values make early
    * iterations count less.
    */
  def getTau0: Double = this.tau0

  /**
    * A (positive) learning parameter that downweights early iterations. Larger values make early
    * iterations count less.
    * Default: 1024, following the original Online LDA paper.
    */
  def setTau0(tau0: Double): this.type = {
    require(tau0 > 0, s"LDA tau0 must be positive, but was set to $tau0")
    this.tau0 = tau0
    this
  }

  /**
    * Learning rate: exponential decay rate
    */
  def getKappa: Double = this.kappa

  /**
    * Learning rate: exponential decay rate---should be between
    * (0.5, 1.0] to guarantee asymptotic convergence.
    * Default: 0.51, based on the original Online LDA paper.
    */
  def setKappa(kappa: Double): this.type = {
    require(kappa >= 0, s"Online LDA kappa must be nonnegative, but was set to $kappa")
    this.kappa = kappa
    this
  }

  /**
    * Mini-batch fraction, which sets the fraction of document sampled and used in each iteration
    */
  def getMiniBatchFraction: Double = this.miniBatchFraction

  /**
    * Mini-batch fraction in (0, 1], which sets the fraction of document sampled and used in
    * each iteration.
    *
    * @note This should be adjusted in synch with `LDA.setMaxIterations()`
    * so the entire corpus is used.  Specifically, set both so that
    * maxIterations * miniBatchFraction is at least 1.
    *
    * Default: 0.05, i.e., 5% of total documents.
    */
  def setMiniBatchFraction(miniBatchFraction: Double): this.type = {
    require(miniBatchFraction > 0.0 && miniBatchFraction <= 1.0,
      s"Online LDA miniBatchFraction must be in range (0,1], but was set to $miniBatchFraction")
    this.miniBatchFraction = miniBatchFraction
    this
  }

  /**
    * Optimize docConcentration, indicates whether docConcentration (Dirichlet parameter for
    * document-topic distribution) will be optimized during training.
    */
  def getOptimizeDocConcentration: Boolean = this.optimizeDocConcentration

  /**
    * Sets whether to optimize docConcentration parameter during training.
    *
    * Default: false
    */
  def setOptimizeDocConcentration(optimizeDocConcentration: Boolean): this.type = {
    this.optimizeDocConcentration = optimizeDocConcentration
    this
  }

  /**
    * Set the Dirichlet parameter for the posterior over topics.
    * This is only used for testing now. In the future, it can help support training stop/resume.
    */
  private[clustering] def setLambda(lambda: BDM[Double]): this.type = {
    this.lambda = lambda
    this
  }

  /**
    * Used for random initialization of the variational parameters.
    * Larger value produces values closer to 1.0.
    * This is only used for testing currently.
    */
  private[clustering] def setGammaShape(shape: Double): this.type = {
    this.gammaShape = shape
    this
  }

  /**
    * Sets whether to sample mini-batches with or without replacement. (default = true)
    * This is only used for testing currently.
    */
  private[clustering] def setSampleWithReplacement(replace: Boolean): this.type = {
    this.sampleWithReplacement = replace
    this
  }

  override private[clustering] def initialize( docs: Array[(Long, BDV[Double])],
                                               lda: LDA): OnlineLDAOptimizer = {
    this.k = lda.getK
    this.corpusSize = docs.length
    this.vocabSize = docs(0)._2.size
    this.alpha = if (lda.getAsymmetricDocConcentration.size == 1) {
      if (lda.getAsymmetricDocConcentration(0) == -1) BDV(Array.fill(k)(1.0 / k))
      else {
        require(lda.getAsymmetricDocConcentration(0) >= 0,
          s"all entries in alpha must be >=0, got: $alpha")
        BDV(Array.fill(k)(lda.getAsymmetricDocConcentration(0)))
      }
    } else {
      require(lda.getAsymmetricDocConcentration.size == k,
        s"alpha must have length k, got: $alpha")
      lda.getAsymmetricDocConcentration.foreach { case  x =>
        require(x >= 0, s"all entries in alpha must be >= 0, got: $alpha")
      }
      lda.getAsymmetricDocConcentration
    }
    this.eta = if (lda.getTopicConcentration == -1) 1.0 / k else lda.getTopicConcentration
    this.randomGenerator = new Random(lda.getSeed)

    this.docs = docs

    // Initialize the variational distribution q(beta|lambda)
    this.lambda = getGammaMatrix(k, vocabSize)
    this.iteration = 0
    this
  }

  override /*private[clustering]*/ def next(): OnlineLDAOptimizer = {
    import scala.util.Random
    val batch = Random.shuffle(docs.toList).take((miniBatchFraction * docs.length).toInt) //TODO fraction -> Int ok? seed?
//    if (batch.size == 0) return this
//    submitMiniBatch(batch)
    this
  }

  /**
    * Submit a subset (like 1%, decide by the miniBatchFraction) of the corpus to the Online LDA
    * model, and it will update the topic distribution adaptively for the terms appearing in the
    * subset.
    */
//  private[clustering] def submitMiniBatch(batch: Array[(Long, BDV[Int])]): OnlineLDAOptimizer = {
//    iteration += 1
//    val k = this.k
//    val vocabSize = this.vocabSize
//    val expElogbeta = exp(LDAUtils.dirichletExpectation(lambda)).t
//    val alpha = this.alpha
//    val gammaShape = this.gammaShape
//
//    val stats: Array[(BDM[Double], List[BDV[Double]])] = batch.map { docs =>
//      val nonEmptyDocs = docs.filter(_._2.numNonzeros > 0)
//
//      val stat = BDM.zeros[Double](k, vocabSize)
//      var gammaPart = List[BDV[Double]]()
//      nonEmptyDocs.foreach { case (_, termCounts: Vector) =>
//        val (gammad, sstats, ids) = OnlineLDAOptimizer.variationalTopicInference(
//          termCounts, expElogbeta, alpha, gammaShape, k)
//        stat(::, ids) := stat(::, ids).toDenseMatrix + sstats
//        gammaPart = gammad :: gammaPart
//      }
//      Iterator((stat, gammaPart))
//    }
//    val statsSum: BDM[Double] = stats.map(_._1).treeAggregate(BDM.zeros[Double](k, vocabSize))(
//      _ += _, _ += _)
//    val gammat: BDM[Double] = breeze.linalg.DenseMatrix.vertcat(
//      stats.map(_._2).flatMap(list => list).collect().map(_.toDenseMatrix): _*)
//
//    val batchResult = statsSum :* expElogbeta.t
//
//    // Note that this is an optimization to avoid batch.count
//    updateLambda(batchResult, (miniBatchFraction * corpusSize).ceil.toInt)
//    if (optimizeDocConcentration) updateAlpha(gammat)
//    this
//  }

  /**
    * Update lambda based on the batch submitted. batchSize can be different for each iteration.
    */
  private def updateLambda(stat: BDM[Double], batchSize: Int): Unit = {
    // weight of the mini-batch.
    val weight = rho()

    // Update lambda based on documents.
    lambda := (1 - weight) * lambda +
      weight * (stat * (corpusSize.toDouble / batchSize.toDouble) + eta)
  }

  /**
    * Update alpha based on `gammat`, the inferred topic distributions for documents in the
    * current mini-batch. Uses Newton-Rhapson method.
    * @see Section 3.3, Huang: Maximum Likelihood Estimation of Dirichlet Distribution Parameters
    *      (http://jonathan-huang.org/research/dirichlet/dirichlet.pdf)
    */
  private def updateAlpha(gammat: BDM[Double]): Unit = {
    val weight = rho()
    val N = gammat.rows.toDouble
    val alpha = this.alpha
    val logphat: BDV[Double] =
      sum(LDAUtils.dirichletExpectation(gammat)(::, breeze.linalg.*)).t / N
    val gradf = N * (-LDAUtils.dirichletExpectation(alpha) + logphat)

    val c = N * trigamma(sum(alpha))
    val q = -N * trigamma(alpha)
    val b = sum(gradf / q) / (1D / c + sum(1D / q))

    val dalpha = -(gradf - b) / q

    if (all((weight * dalpha + alpha) :> 0D)) {
      alpha :+= weight * dalpha
      this.alpha = BDV(alpha.toArray)
    }
  }


  /** Calculate learning rate rho for the current [[iteration]]. */
  private def rho(): Double = {
    math.pow(getTau0 + this.iteration, -getKappa)
  }

  /**
    * Get a random matrix to initialize lambda.
    */
  private def getGammaMatrix(row: Int, col: Int): BDM[Double] = {
    val randBasis = new RandBasis(new org.apache.commons.math3.random.MersenneTwister(
      randomGenerator.nextLong()))
    val gammaRandomGenerator = new Gamma(gammaShape, 1.0 / gammaShape)(randBasis)
    val temp = gammaRandomGenerator.sample(row * col).toArray
    new BDM[Double](col, row, temp).t
  }

  override private[clustering] def getLDAModel(iterationTimes: Array[Double]): LDAModel = {
    new LocalLDAModel(lambda.t, alpha, eta, gammaShape)
  }

}

/**
  * Serializable companion object containing helper methods and shared code for
  * [[OnlineLDAOptimizer]] and [[LocalLDAModel]].
  */
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
                                                     termCounts: BDV[Double],
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
      new Gamma(gammaShape, 1.0 / gammaShape).samplesVector(k)                   // K
    val expElogthetad: BDV[Double] = exp(LDAUtils.dirichletExpectation(gammad))  // K
    val expElogbetad = expElogbeta(ids, ::).toDenseMatrix                        // ids * K

    val phiNorm: BDV[Double] = expElogbetad * expElogthetad :+ 1e-100            // ids
    var meanGammaChange = 1D
    val ctsVector = new BDV[Double](cts)                                         // ids

    // Iterate between gamma and phi until convergence
    while (meanGammaChange > 1e-3) {
      val lastgamma = gammad.copy
      //        K                  K * ids               ids
      gammad := (expElogthetad :* (expElogbetad.t * (ctsVector :/ phiNorm))) :+ alpha
      expElogthetad := exp(LDAUtils.dirichletExpectation(gammad))
      // TODO: Keep more values in log space, and only exponentiate when needed.
      phiNorm := expElogbetad * expElogthetad :+ 1e-100
      meanGammaChange = sum(abs(gammad - lastgamma)) / k
    }

    val sstatsd = expElogthetad.asDenseMatrix.t * (ctsVector :/ phiNorm).asDenseMatrix
    (gammad, sstatsd, ids)
  }
}

