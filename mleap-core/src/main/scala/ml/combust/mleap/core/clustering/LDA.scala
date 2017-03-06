package ml.combust.mleap.core.clustering

import java.util.Random

import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.clustering.optimization.{LDAOptimizer, OnlineLDAOptimizer}
/**
  * Created by mageswarand on 15/2/17.
  */

/**
  * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
  *
  * Terminology:
  *  - "word" = "term": an element of the vocabulary
  *  - "token": instance of a term appearing in a document
  *  - "topic": multinomial distribution over words representing some concept
  *
  * References:
  *  - Original LDA paper (journal version):
  *    Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.
  *
  * @see <a href="http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation">
  * Latent Dirichlet allocation (Wikipedia)</a>
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/mllib/clustering/LDA.scala")
class LDA private (
                    private var k: Int,
                    private var maxIterations: Int,
                    private var docConcentration: BDV[Double],
                    private var topicConcentration: Double,
                    private var seed: Long,
                    private var checkpointInterval: Int,
                    private var ldaOptimizer: LDAOptimizer) /*extends Logging*/ {

  /**
    * Constructs a LDA instance with default parameters.
    */
  def this() = this(k = 10, maxIterations = 20, docConcentration = BDV(Array[Double](-1)),
    topicConcentration = -1, seed = new Random().nextLong(), checkpointInterval = 10,
    ldaOptimizer = new OnlineLDAOptimizer)

  /**
    * Number of topics to infer, i.e., the number of soft cluster centers.
    */
  def getK: Int = k

  /**
    * Set the number of topics to infer, i.e., the number of soft cluster centers.
    * (default = 10)
    */
  def setK(k: Int): this.type = {
    require(k > 0, s"LDA k (number of clusters) must be > 0, but was set to $k")
    this.k = k
    this
  }

  /**
    * Concentration parameter (commonly named "alpha") for the prior placed on documents'
    * distributions over topics ("theta").
    *
    * This is the parameter to a Dirichlet distribution.
    */
  def getAsymmetricDocConcentration: BDV[Double] = this.docConcentration

  /**
    * Concentration parameter (commonly named "alpha") for the prior placed on documents'
    * distributions over topics ("theta").
    *
    * This method assumes the Dirichlet distribution is symmetric and can be described by a single
    * [[Double]] parameter. It should fail if docConcentration is asymmetric.
    */
  def getDocConcentration: Double = {
    val parameter = docConcentration(0)
    if (docConcentration.size == 1) {
      parameter
    } else {
      require(docConcentration.toArray.forall(_ == parameter))
      parameter
    }
  }

  /**
    * Concentration parameter (commonly named "alpha") for the prior placed on documents'
    * distributions over topics ("theta").
    *
    * This is the parameter to a Dirichlet distribution, where larger values mean more smoothing
    * (more regularization).
    *
    * If set to a singleton vector Vector(-1), then docConcentration is set automatically. If set to
    * singleton vector Vector(t) where t != -1, then t is replicated to a vector of length k during
    * `LDAOptimizer.initialize()`. Otherwise, the [[docConcentration]] vector must be length k.
    * (default = Vector(-1) = automatic)
    *
    * Optimizer-specific parameter settings:
    *  - EM
    *     - Currently only supports symmetric distributions, so all values in the vector should be
    *       the same.
    *     - Values should be greater than 1.0
    *     - default = uniformly (50 / k) + 1, where 50/k is common in LDA libraries and +1 follows
    *       from Asuncion et al. (2009), who recommend a +1 adjustment for EM.
    *  - Online
    *     - Values should be greater than or equal to 0
    *     - default = uniformly (1.0 / k), following the implementation from
    *       <a href="https://github.com/Blei-Lab/onlineldavb">here</a>.
    */
  def setDocConcentration(docConcentration: BDV[Double]): this.type = {
    require(docConcentration.size == 1 || docConcentration.size == k,
      s"Size of docConcentration must be 1 or ${k} but got ${docConcentration.size}")
    this.docConcentration = docConcentration
    this
  }

  /**
    * Replicates a [[Double]] docConcentration to create a symmetric prior.
    */
  def setDocConcentration(docConcentration: Double): this.type = {
    this.docConcentration = BDV(Array(docConcentration))
    this
  }

  /**
    * Alias for [[getAsymmetricDocConcentration]]
    */
  def getAsymmetricAlpha: BDV[Double] = getAsymmetricDocConcentration

  /**
    * Alias for [[getDocConcentration]]
    */
  def getAlpha: Double = getDocConcentration

  /**
    * Alias for `setDocConcentration()`
    */
  def setAlpha(alpha: BDV[Double]): this.type = setDocConcentration(alpha)

  /**
    * Alias for `setDocConcentration()`
    */
  def setAlpha(alpha: Double): this.type = setDocConcentration(alpha)

  /**
    * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
    * distributions over terms.
    *
    * This is the parameter to a symmetric Dirichlet distribution.
    *
    * @note The topics' distributions over terms are called "beta" in the original LDA paper
    * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
    */
  def getTopicConcentration: Double = this.topicConcentration

  /**
    * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
    * distributions over terms.
    *
    * This is the parameter to a symmetric Dirichlet distribution.
    *
    * @note The topics' distributions over terms are called "beta" in the original LDA paper
    * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
    *
    * If set to -1, then topicConcentration is set automatically.
    *  (default = -1 = automatic)
    *
    * Optimizer-specific parameter settings:
    *  - EM
    *     - Value should be greater than 1.0
    *     - default = 0.1 + 1, where 0.1 gives a small amount of smoothing and +1 follows
    *       Asuncion et al. (2009), who recommend a +1 adjustment for EM.
    *  - Online
    *     - Value should be greater than or equal to 0
    *     - default = (1.0 / k), following the implementation from
    *       <a href="https://github.com/Blei-Lab/onlineldavb">here</a>.
    */
  def setTopicConcentration(topicConcentration: Double): this.type = {
    this.topicConcentration = topicConcentration
    this
  }

  /**
    * Alias for [[getTopicConcentration]]
    */
  def getBeta: Double = getTopicConcentration

  /**
    * Alias for `setTopicConcentration()`
    */
  def setBeta(beta: Double): this.type = setTopicConcentration(beta)

  /**
    * Maximum number of iterations allowed.
    */
  def getMaxIterations: Int = maxIterations

  /**
    * Set the maximum number of iterations allowed.
    * (default = 20)
    */
  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations >= 0,
      s"Maximum of iterations must be nonnegative but got ${maxIterations}")
    this.maxIterations = maxIterations
    this
  }

  /**
    * Random seed for cluster initialization.
    */
  def getSeed: Long = seed

  /**
    * Set the random seed for cluster initialization.
    */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
    * Period (in iterations) between checkpoints.
    */
  def getCheckpointInterval: Int = checkpointInterval

  /**
    * Parameter for set checkpoint interval (greater than or equal to 1) or disable checkpoint (-1).
    * E.g. 10 means that the cache will get checkpointed every 10 iterations. Checkpointing helps
    * with recovery (when nodes fail). It also helps with eliminating temporary shuffle files on
    * disk, which can be important when LDA is run for many iterations. If the checkpoint directory
    * is not set in [[org.apache.spark.SparkContext]], this setting is ignored. (default = 10)
    *
    * @see [[org.apache.spark.SparkContext#setCheckpointDir]]
    */
  def setCheckpointInterval(checkpointInterval: Int): this.type = {
    require(checkpointInterval == -1 || checkpointInterval > 0,
      s"Period between checkpoints must be -1 or positive but got ${checkpointInterval}")
    this.checkpointInterval = checkpointInterval
    this
  }


  /**
    * :: DeveloperApi ::
    *
    * LDAOptimizer used to perform the actual calculation
    */
  @DeveloperApi
  def getOptimizer: LDAOptimizer = ldaOptimizer

  /**
    * :: DeveloperApi ::
    *
    * LDAOptimizer used to perform the actual calculation (default = EMLDAOptimizer)
    */
  @DeveloperApi
  def setOptimizer(optimizer: LDAOptimizer): this.type = {
    this.ldaOptimizer = optimizer
    this
  }

  /**
    * Set the LDAOptimizer used to perform the actual calculation by algorithm name.
    * Currently "em", "online" are supported.
    */
//  def setOptimizer(optimizerName: String): this.type = {
//    this.ldaOptimizer =
//      optimizerName.toLowerCase match {
//        case "em" => throw new IllegalArgumentException(s"Only online is supported but got $other.")
//        case "online" => new OnlineLDAOptimizer
//        case other =>
//          throw new IllegalArgumentException(s"Only em, online are supported but got $other.")
//      }
//    this
//  }

  /**
    * Learn an LDA model using the given dataset.
    *
    * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
    *                   The term count vectors are "bags of words" with a fixed-size vocabulary
    *                   (where the vocabulary size is the length of the vector).
    *                   Document IDs must be unique and greater than or equal to 0.
    * @return  Inferred LDA model
    */
  def run(documents: Array[(Long, BDV[Double])]): LDAModel = {
    val state = ldaOptimizer.initialize(documents, this)
    var iter = 0
    val iterationTimes = Array.fill[Double](maxIterations)(0)
    while (iter < maxIterations) {
      val start = System.nanoTime()
      state.next()
      val elapsedSeconds = (System.nanoTime() - start) / 1e9
      iterationTimes(iter) = elapsedSeconds
      iter += 1
    }
    state.getLDAModel(iterationTimes)
  }

}


private[clustering] object LDA {

  /*
    DEVELOPERS NOTE:

    This implementation uses GraphX, where the graph is bipartite with 2 types of vertices:
     - Document vertices
        - indexed with unique indices >= 0
        - Store vectors of length k (# topics).
     - Term vertices
        - indexed {-1, -2, ..., -vocabSize}
        - Store vectors of length k (# topics).
     - Edges correspond to terms appearing in documents.
        - Edges are directed Document -> Term.
        - Edges are partitioned by documents.

    Info on EM implementation.
     - We follow Section 2.2 from Asuncion et al., 2009.  We use some of their notation.
     - In this implementation, there is one edge for every unique term appearing in a document,
       i.e., for every unique (document, term) pair.
     - Notation:
        - N_{wkj} = count of tokens of term w currently assigned to topic k in document j
        - N_{*} where * is missing a subscript w/k/j is the count summed over missing subscript(s)
        - gamma_{wjk} = P(z_i = k | x_i = w, d_i = j),
          the probability of term x_i in document d_i having topic z_i.
     - Data graph
        - Document vertices store N_{kj}
        - Term vertices store N_{wk}
        - Edges store N_{wj}.
        - Global data N_k
     - Algorithm
        - Initial state:
           - Document and term vertices store random counts N_{wk}, N_{kj}.
        - E-step: For each (document,term) pair i, compute P(z_i | x_i, d_i).
           - Aggregate N_k from term vertices.
           - Compute gamma_{wjk} for each possible topic k, from each triplet.
             using inputs N_{wk}, N_{kj}, N_k.
        - M-step: Compute sufficient statistics for hidden parameters phi and theta
          (counts N_{wk}, N_{kj}, N_k).
           - Document update:
              - N_{kj} <- sum_w N_{wj} gamma_{wjk}
              - N_j <- sum_k N_{kj}  (only needed to output predictions)
           - Term update:
              - N_{wk} <- sum_j N_{wj} gamma_{wjk}
              - N_k <- sum_w N_{wk}

    TODO: Add simplex constraints to allow alpha in (0,1).
          See: Vorontsov and Potapenko. "Tutorial on Probabilistic Topic Modeling : Additive
               Regularization for Stochastic Matrix Factorization." 2014.
   */

  /**
    * Vector over topics (length k) of token counts.
    * The meaning of these counts can vary, and it may or may not be normalized to be a distribution.
    */
  private[clustering] type TopicCounts = BDV[Double]

  private[clustering] type TokenCount = Double

  /** Term vertex IDs are {-1, -2, ..., -vocabSize} */
  private[clustering] def term2index(term: Int): Long = -(1 + term.toLong)

  private[clustering] def index2term(termIndex: Long): Int = -(1 + termIndex).toInt

  private[clustering] def isDocumentVertex(v: (VertexId, _)): Boolean = v._1 >= 0

  private[clustering] def isTermVertex(v: (VertexId, _)): Boolean = v._1 < 0

  /**
    * Compute gamma_{wjk}, a distribution over topics k.
    */
  private[clustering] def computePTopic(
                                         docTopicCounts: TopicCounts,
                                         termTopicCounts: TopicCounts,
                                         totalTopicCounts: TopicCounts,
                                         vocabSize: Int,
                                         eta: Double,
                                         alpha: Double): TopicCounts = {
    val K = docTopicCounts.length
    val N_j = docTopicCounts.data
    val N_w = termTopicCounts.data
    val N = totalTopicCounts.data
    val eta1 = eta - 1.0
    val alpha1 = alpha - 1.0
    val Weta1 = vocabSize * eta1
    var sum = 0.0
    val gamma_wj = new Array[Double](K)
    var k = 0
    while (k < K) {
      val gamma_wjk = (N_w(k) + eta1) * (N_j(k) + alpha1) / (N(k) + Weta1)
      gamma_wj(k) = gamma_wjk
      sum += gamma_wjk
      k += 1
    }
    // normalize
    BDV(gamma_wj) /= sum
  }
}
