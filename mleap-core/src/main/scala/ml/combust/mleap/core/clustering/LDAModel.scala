package ml.combust.mleap.core.clustering

//Referemce
//MLeap Model
import ml.combust.mleap.core.clustering.optimization.OnlineLDAOptimizer
//import ml.combust.mleap.core.clustering.optimization.LDAOptimizer


//Spark Model
//MLeap Model

//Mleap Loader/CT

//Spark SerDer
//MLeap SerDer

/////

//import ml.combust.mleap.core.clustering.{LDA, LDAModel, LDAUtils, LocalLDAModel}

/**
  * Created by mageswarand on 15/2/17.
  *
  *
  */


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

  /* TODO (once LDA can be trained with Strings or given a dictionary)
   * Return the topics described by weighted terms.
   *
   * This is similar to [[describeTopics()]] but returns String values for terms.
   * If this model was trained using Strings or was given a dictionary, then this method returns
   * terms as text.  Otherwise, this method returns terms as term indices.
   *
   * This limits the number of terms per topic.
   * This is approximate; it may not return exactly the top-weighted terms for each topic.
   * To get a more precise set of top terms, increase maxTermsPerTopic.
   *
   * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
   * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
   *          (terms, term weights in topic) where terms are either the actual term text
   *          (if available) or the term indices.
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  // def describeTopicsAsStrings(maxTermsPerTopic: Int): Array[(Array[Double], Array[String])]

  /* TODO (once LDA can be trained with Strings or given a dictionary)
   * Return the topics described by weighted terms.
   *
   * This is similar to [[describeTopics()]] but returns String values for terms.
   * If this model was trained using Strings or was given a dictionary, then this method returns
   * terms as text.  Otherwise, this method returns terms as term indices.
   *
   * WARNING: If vocabSize and k are large, this can return a large object!
   *
   * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
   *          (terms, term weights in topic) where terms are either the actual term text
   *          (if available) or the term indices.
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  // def describeTopicsAsStrings(): Array[(Array[Double], Array[String])] =
  //  describeTopicsAsStrings(vocabSize)

  /* TODO
   * Compute the log likelihood of the observed tokens, given the current parameter estimates:
   *  log P(docs | topics, topic distributions for docs, alpha, eta)
   *
   * Note:
   *  - This excludes the prior.
   *  - Even with the prior, this is NOT the same as the data log likelihood given the
   *    hyperparameters.
   *
   * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
   *                   The term count vectors are "bags of words" with a fixed-size vocabulary
   *                   (where the vocabulary size is the length of the vector).
   *                   This must use the same vocabulary (ordering of term counts) as in training.
   *                   Document IDs must be unique and >= 0.
   * @return  Estimated log likelihood of the data under this model
   */
  // def logLikelihood(documents: RDD[(Long, Vector)]): Double

  /* TODO
   * Compute the estimated topic distribution for each document.
   * This is often called 'theta' in the literature.
   *
   * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
   *                   The term count vectors are "bags of words" with a fixed-size vocabulary
   *                   (where the vocabulary size is the length of the vector).
   *                   This must use the same vocabulary (ordering of term counts) as in training.
   *                   Document IDs must be unique and greater than or equal to 0.
   * @return  Estimated topic distribution for each document.
   *          The returned RDD may be zipped with the given RDD, where each returned vector
   *          is a multinomial distribution over topics.
   */
  // def topicDistributions(documents: RDD[(Long, Vector)]): RDD[(Long, Vector)]

}

/**
  * Local LDA model.
  * This model stores only the inferred topics.
  *
  * @param topics Inferred topics (vocabSize x k matrix).
  */
class LocalLDAModel ( val topics: Matrix[Double],
                      override val docConcentration: BDV[Double],
                      override val topicConcentration: Double,
                      override protected val gammaShape: Double = 100)
  extends LDAModel {

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


  // TODO: declare in LDAModel and override once implemented in DistributedLDAModel
  /**
    * Calculates a lower bound on the log likelihood of the entire corpus.
    *
    * See Equation (16) in original Online LDA paper.
    *
    * @param documents test corpus to use for calculating log likelihood
    * @return variational lower bound on the log likelihood of the entire corpus
    */
  def logLikelihood(documents: Array[(Long, BDV[Double])]): Double = logLikelihoodBound(documents,
    docConcentration, topicConcentration, topicsMatrix.toDenseMatrix, gammaShape, k,
    vocabSize)

  /**
    * Calculate an upper bound bound on perplexity.  (Lower is better.)
    * See Equation (16) in original Online LDA paper.
    *
    * @param documents test corpus to use for calculating perplexity
    * @return Variational upper bound on log perplexity per token.
    */
  def logPerplexity(documents: Array[(Long, BDV[Double])]): Double = {
    //    val corpusTokenCount = documents
    //      .map { case (_, termCounts) => termCounts.toArray.sum }
    //      .sum()
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
                                  documents: Array[(Long, BDV[Double])],
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
    val corpusPart: Double =
    documents./*filter(_._2.numNonzeros > 0).*/map { case (id: Long, termCounts: BDV[Double]) =>
      val localElogbeta = Elogbeta
      var docBound = 0.0D
      val (gammad: BDV[Double], _, _) = OnlineLDAOptimizer.variationalTopicInference(
        termCounts, exp(localElogbeta), brzAlpha, gammaShape, k)
      val Elogthetad: BDV[Double] = LDAUtils.dirichletExpectation(gammad)

      // E[log p(doc | theta, beta)]
      termCounts.foreachPair{  case (idx, count) =>
        docBound += count * LDAUtils.logSumExp(Elogthetad + localElogbeta(idx, ::).t)
      }
//      termCounts.foreach { case count =>
//        docBound += count * LDAUtils.logSumExp(Elogthetad + localElogbeta(0, ::).t) //TODO 0 -> idx
//      }

      // E[log p(theta | alpha) - log q(theta | gamma)]
      docBound += sum((brzAlpha - gammad) :* Elogthetad)
      docBound += sum(lgamma(gammad) - lgamma(brzAlpha))
      docBound += lgamma(sum(brzAlpha)) - lgamma(sum(gammad))

      docBound
    }.reduce(_ + _)//.sum() //TODO reduce ok?

    // Bound component for prob(topic-term distributions):
    //   E[log p(beta | eta) - log q(beta | lambda)]
    val sumEta = eta * vocabSize
    val topicsPart: Double = sum((eta - lambda) :* Elogbeta) +
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
  def topicDistributions(documents: Array[(Long, BDV[Double])]): Array[(Long, BDV[Double])] = {
    // Double transpose because dirichletExpectation normalizes by row and we need to normalize
    // by topic (columns of lambda)
    val expElogbeta = exp(LDAUtils.dirichletExpectation(topicsMatrix.toDenseMatrix.t).t)
    val docConcentrationBrz = this.docConcentration
    val gammaShape = this.gammaShape
    val k = this.k

    documents.map { case (id: Long, termCounts: BDV[Double]) =>
      if (termCounts.size == 0) {
        (id, BDV(Array.fill[Double](k)(0)))
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
  private def getTopicDistributionMethod(): BDV[Double] => BDV[Double]  = {
    val expElogbeta = exp(LDAUtils.dirichletExpectation(topicsMatrix.toDenseMatrix.t).t)
    val docConcentrationBrz = this.docConcentration
    val gammaShape = this.gammaShape
    val k = this.k

    (termCounts: BDV[Double]) =>
      if (termCounts.size == 0) {
        BDV(Array.fill[Double](k)(0))
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
  def topicDistribution(document: BDV[Double]): BDV[Double] = {
    val expElogbeta = exp(LDAUtils.dirichletExpectation(topicsMatrix.toDenseMatrix.t).t)
    if (document.size == 0) {
      BDV(Array.fill[Double](this.k)(0))
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

/**
  * Local (non-distributed) model fitted by [[LDA]].
  *
  * This model stores the inferred topics only; it does not store info about the training dataset.
  */
object LocalLDAModel