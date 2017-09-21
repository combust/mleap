package ml.combust.mleap.core.clustering

import breeze.linalg.{Matrix, Vector, normalize, sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{exp, lgamma}
import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.clustering.optimization.OnlineLDAOptimizer
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}

/**
  * Created by mageswarand on 15/2/17.
  *
  * Note: All Spark Linear Algebra APIs are converted to bare Breeze APIs
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/clustering/LDAModel.scala")
abstract class LDAModel private[clustering] {

  /** Number of topics */
  def k: Int

  /** Vocabulary size (number of terms or terms in the vocabulary) */
  def vocabSize: Int

  /**
    * Concentration parameter (commonly named "alpha") for the prior placed on documents'
    * distributions over topics ("theta").
    *
    * This is the parameter to a Dirichlet distribution.
    */
  def docConcentration: Vector[Double]

  /**
    * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
    * distributions over terms.
    *
    * This is the parameter to a symmetric Dirichlet distribution.
    *
    * @note The topics' distributions over terms are called "beta" in the original LDA paper
    * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
    */
  def topicConcentration: Double

  /**
    * Shape parameter for random initialization of variational parameter gamma.
    * Used for variational inference for perplexity and other test-time computations.
    */
  protected def gammaShape: Double

  /**
    * Inferred topics, where each topic is represented by a distribution over terms.
    * This is a matrix of size vocabSize x k, where each column is a topic.
    * No guarantees are given about the ordering of the topics.
    */
  def topicsMatrix: Matrix[Double]

  /**
    * Return the topics described by weighted terms.
    *
    * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
    * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
    *          (term indices, term weights in topic).
    *          Each topic's terms are sorted in order of decreasing weight.
    */
  def describeTopics(maxTermsPerTopic: Int): Array[(Array[Int], Array[Double])]

  /**
    * Return the topics described by weighted terms.
    *
    * WARNING: If vocabSize and k are large, this can return a large object!
    *
    * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
    *          (term indices, term weights in topic).
    *          Each topic's terms are sorted in order of decreasing weight.
    */
  def describeTopics(): Array[(Array[Int], Array[Double])] = describeTopics(vocabSize)
}

/**
  * Local LDA model.
  * This model stores only the inferred topics.
  *
  * @param topics Inferred topics (vocabSize x k matrix).
  */
case class LocalLDAModel (topics: Matrix[Double],
                          override val docConcentration: BDV[Double],
                          override val topicConcentration: Double,
                          protected val gammaShape: Double = 100)
  extends LDAModel with Model {

  override def k: Int = topics.cols

  override def vocabSize: Int = topics.rows

  override def topicsMatrix: Matrix[Double] = topics

  override def describeTopics(maxTermsPerTopic: Int): Array[(Array[Int], Array[Double])] = {
    val brzTopics = topics.toDenseMatrix
    Range(0, k).map { topicIndex =>
      val topic = normalize(brzTopics(::, topicIndex), 1.0)
      val (termWeights, terms) =
        topic.toArray.zipWithIndex.sortBy(-_._1).take(maxTermsPerTopic).unzip
      (terms.toArray, termWeights.toArray)
    }.toArray
  }

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(k)).get

  override def outputSchema: StructType = StructType("prediction" -> TensorType.Double(k)).get

  /**
    * Calculates a lower bound on the log likelihood of the entire corpus.
    *
    * TODO: declare in LDAModel and override once implemented in DistributedLDAModel
    * See Equation (16) in original Online LDA paper.
    *
    * @param documents test corpus to use for calculating log likelihood
    * @return variational lower bound on the log likelihood of the entire corpus
    */
  def logLikelihood(documents: Array[(Long, Vector[Double])]): Double = logLikelihoodBound(documents,
    docConcentration, topicConcentration, topicsMatrix.toDenseMatrix, gammaShape, k,
    vocabSize)
  /**
    * Calculate an upper bound bound on perplexity.  (Lower is better.)
    * See Equation (16) in original Online LDA paper.
    *
    * @param documents test corpus to use for calculating perplexity
    * @return Variational upper bound on log perplexity per token.
    */
  def logPerplexity(documents: Array[(Long, Vector[Double])]): Double = {
    val corpusTokenCount = documents
      .map { case (_, termCounts) => sum(termCounts) }

    -logLikelihood(documents) / sum(corpusTokenCount)
  }
  /**
    * Estimate the variational likelihood bound of from `documents`:
    *    log p(documents) >= E_q[log p(documents)] - E_q[log q(documents)]
    * This bound is derived by decomposing the LDA model to:
    *    log p(documents) = E_q[log p(documents)] - E_q[log q(documents)] + D(q|p)
    * and noting that the KL-divergence D(q|p) >= 0.
    *
    * See Equation (16) in original Online LDA paper, as well as Appendix A.3 in the JMLR version of
    * the original LDA paper.
    * @param documents a subset of the test corpus
    * @param alpha document-topic Dirichlet prior parameters
    * @param eta topic-word Dirichlet prior parameter
    * @param lambda parameters for variational q(beta | lambda) topic-word distributions
    * @param gammaShape shape parameter for random initialization of variational q(theta | gamma)
    *                   topic mixture distributions
    * @param k number of topics
    * @param vocabSize number of unique terms in the entire test corpus
    */
  private def logLikelihoodBound(
                                  documents: Array[(Long, Vector[Double])],
                                  alpha: BDV[Double],
                                  eta: Double,
                                  lambda: BDM[Double],
                                  gammaShape: Double,
                                  k: Int,
                                  vocabSize: Long): Double = {
    val brzAlpha = alpha.toDenseVector
    // transpose because dirichletExpectation normalizes by row and we need to normalize
    // by topic (columns of lambda)
    val Elogbeta = LDAUtils.dirichletExpectation(lambda.t).t

    // Sum bound components for each document:
    //  component for prob(tokens) + component for prob(document-topic distribution)
    val corpusPart =
    documents.filter{case (id: Long, termCounts: Vector[Double]) =>
      var nnz = 0
      termCounts.values.foreach { v =>
        if (v != 0.0) {
          nnz += 1
        }
      }
      nnz > 0
    }.map { case (id: Long, termCounts: Vector[Double]) =>
      val localElogbeta = Elogbeta //TODO
    var docBound = 0.0D
      val (gammad: BDV[Double], _, _) = OnlineLDAOptimizer.variationalTopicInference(
        termCounts, exp(localElogbeta), brzAlpha, gammaShape, k)
      val Elogthetad: BDV[Double] = LDAUtils.dirichletExpectation(gammad)

      // E[log p(doc | theta, beta)]
      termCounts.foreachPair { case (idx, count) => //TODO check foreachActive-> foreachPair
        docBound += count * LDAUtils.logSumExp(Elogthetad + localElogbeta(idx, ::).t)
      }
      // E[log p(theta | alpha) - log q(theta | gamma)]
      docBound += sum((brzAlpha - gammad) :* Elogthetad)
      docBound += sum(lgamma(gammad) - lgamma(brzAlpha))
      docBound += lgamma(sum(brzAlpha)) - lgamma(sum(gammad))

      docBound
    }.reduce(_ + _)

    // Bound component for prob(topic-term distributions):
    //   E[log p(beta | eta) - log q(beta | lambda)]
    val sumEta = eta * vocabSize
    val topicsPart = sum((eta - lambda) :* Elogbeta) +
      sum(lgamma(lambda) - lgamma(eta)) +
      sum(lgamma(sumEta) - lgamma(sum(lambda(::, breeze.linalg.*))))

    corpusPart + topicsPart
  }

  /**
    * Predicts the topic mixture distribution for each document (often called "theta" in the
    * literature).  Returns a vector of zeros for an empty document.
    *
    * This uses a variational approximation following Hoffman et al. (2010), where the approximate
    * distribution is called "gamma."  Technically, this method returns this approximation "gamma"
    * for each document.
    * @param documents documents to predict topic mixture distributions for
    * @return An RDD of (document ID, topic mixture distribution for document)
    */
  // TODO: declare in LDAModel and override once implemented in DistributedLDAModel
  def topicDistributions(documents: Array[(Long, Vector[Double])]): Array[(Long, Vector[Double])] = {
    // Double transpose because dirichletExpectation normalizes by row and we need to normalize
    // by topic (columns of lambda)
    val expElogbeta = exp(LDAUtils.dirichletExpectation(topicsMatrix.toDenseMatrix.t).t)
    val docConcentrationBrz = this.docConcentration
    val gammaShape = this.gammaShape
    val k = this.k

    documents.map { case (id: Long, termCounts: Vector[Double]) =>
      var nnz = 0
      if (termCounts.toArray.filter(_!=0).size == 0) {
        (id, BDV.zeros[Double](k))
      } else {
        val (gamma, _, _) = OnlineLDAOptimizer.variationalTopicInference(
          termCounts,
          expElogbeta,
          docConcentrationBrz,
          gammaShape,
          k)
        (id, BDV(normalize(gamma, 1.0).toArray))
      }
    }
  }


  /** Get a method usable as a UDF for [[topicDistributions()]] */
  private def getTopicDistributionMethod():  Vector[Double] => Vector[Double]   = {
    val expElogbeta = exp(LDAUtils.dirichletExpectation(topicsMatrix.toDenseMatrix.t).t)
    val docConcentrationBrz = this.docConcentration
    val gammaShape = this.gammaShape
    val k = this.k

    (termCounts: Vector[Double]) =>
      if (termCounts.toArray.filter(_ != 0).size == 0) {
        BDV.zeros[Double](k)
      } else {
        val (gamma, _, _) = OnlineLDAOptimizer.variationalTopicInference(
          termCounts,
          expElogbeta,
          docConcentrationBrz,
          gammaShape,
          k)
        BDV(normalize(gamma, 1.0).toArray)
      }
  }

  /**
    * Predicts the topic mixture distribution for a document (often called "theta" in the
    * literature).  Returns a vector of zeros for an empty document.
    *
    * Note this means to allow quick query for single document. For batch documents, please refer
    * to `topicDistributions()` to avoid overhead.
    *
    * @param document document to predict topic mixture distributions for
    * @return topic mixture distribution for the document
    */
  def topicDistribution(document: Vector[Double]): Vector[Double] = {
    val expElogbeta = exp(LDAUtils.dirichletExpectation(topicsMatrix.toDenseMatrix.t).t)
    if (document.toArray.filter(_ != 0).size == 0) {
      BDV.zeros[Double](this.k)
    } else {
      val (gamma, _, _) = OnlineLDAOptimizer.variationalTopicInference(
        document,
        expElogbeta,
        this.docConcentration,
        gammaShape,
        this.k)
      BDV(normalize(gamma, 1.0).toArray)
    }
  }
}

object LocalLDAModel