package ml.combust.bundle.dsl

import java.nio.file.{FileSystem, Path}
import java.time
import java.util.UUID
import java.time.LocalDateTime

import ml.combust.mleap.BuildValues
import ml.combust.bundle.{BundleContext, BundleRegistry}
import ml.combust.bundle.serializer._

/** Companion class for constants and constructors of [[Bundle]] objects.
  *
  * Contains file names for bundle JSON files and model JSON files.
  */
object Bundle {
  val version = BuildValues.version
  val bundleJson = "bundle.json"
  val root = "root"

  def nodeFile(implicit sc: SerializationContext): String = formattedFile("node")
  def modelFile(implicit sc: SerializationContext): String = formattedFile("model")

  def formattedFile(base: String)(implicit sc: SerializationContext): String = sc.concrete match {
    case SerializationFormat.Json => s"$base.json"
    case SerializationFormat.Protobuf => s"$base.pb"
  }

  def node(name: String): String = s"$name.node"
  def model(name: String): String = s"$name.model"

  object BuiltinOps {
    object regression {
      val linear_regression = "linear_regression"
      val decision_tree_regression = "decision_tree_regression"
      val random_forest_regression = "random_forest_regression"
      val gbt_regression = "gbt_regression"
      val isotonic_regression = "isotonic_regression"
      val aft_survival_regression = "aft_survival_regression"
      val generalized_linear_regression = "generalized_linear_regression"
    }

    object feature {
      val binarizer = "binarizer"
      val coalesce = "coalesce"
      val count_vectorizer = "count_vectorizer"
      val dct = "dct"
      val min_hash_lsh = "min_hash_lsh"
      val bucketed_random_projection_lsh = "bucketed_random_projection_lsh"
      val math_unary = "math_unary"
      val math_binary = "math_binary"
      val string_indexer = "string_indexer"
      val chi_sq_selector = "chi_sq_selector"
      val reverse_string_indexer = "reverse_string_indexer"
      val hashing_term_frequency = "hashing_term_frequency"
      val imputer = "imputer"
      val standard_scaler = "standard_scaler"
      val tokenizer = "tokenizer"
      val vector_assembler = "vector_assembler"
      val one_hot_encoder = "one_hot_encoder"
      val min_max_scaler = "min_max_scaler"
      val max_abs_scaler = "max_abs_scaler"
      val bucketizer = "bucketizer"
      val idf = "idf"
      val string_map = "string_map"
      val elementwise_product = "elementwise_product"
      val normalizer = "normalizer"
      val pca = "pca"
      val ngram = "ngram"
      val vector_slicer = "vector_slicer"
      val vector_indexer = "vector_indexer"
      val polynomial_expansion = "polynomial_expansion"
      val stopwords_remover = "stopwords_remover"
      val word_to_vector = "word_to_vector"
      val multinomial_labeler = "multinomial_labeler"
      val regex_tokenizer = "regex_tokenizer"
      val word_filter = "word_filter"
      val interaction = "interaction"
    }

    object classification {
      val naive_bayes = "naive_bayes"
      val logistic_regression = "logistic_regression"
      val random_forest_classifier = "random_forest_classifier"
      val gbt_classifier = "gbt_classifier"
      val multi_layer_perceptron_classifier = "multi_layer_perceptron_classifier"
      val decision_tree_classifier = "decision_tree_classifier"
      val support_vector_machine = "support_vector_machine"
      val one_vs_rest = "one_vs_rest"
    }

    object clustering {
      val gaussian_mixture = "gaussian_mixture"
      val bisecting_k_means = "bisecting_k_means"
      val k_means = "k_means"
      val lda = "lda_local_model_op"
    }

    val pipeline = "pipeline"
    val tensorflow = "tensorflow"
  }

  def apply[Transformer <: AnyRef](name: String,
                                   format: SerializationFormat,
                                   root: Transformer): Bundle[Transformer] = {
    apply(BundleInfo(uid = UUID.randomUUID(),
      name = name,
      format = format,
      version = Bundle.version,
      LocalDateTime.now()), root)
  }
}

/** Information data for a bundle.
  *
  * @param uid uid for the bundle
  * @param name name of the bundle
  * @param format serialization format of the [[Bundle]]
  * @param version Bundle.ML version used for serializing
  * @param timeCreated LocalDateTime when the model was created
  */
case class BundleInfo(uid: UUID,
                      name: String,
                      format: SerializationFormat,
                      version: String,
                      timeCreated: LocalDateTime)

/** Root object for serializing Bundle.ML pipelines and graphs.
  *
  * @param info info data for the bundle
  * @param root root transformer node
  */
case class Bundle[Transformer <: AnyRef](info: BundleInfo,
                                         root: Transformer) {
  /** Create a [[BundleContext]] for serializing to Bundle.ML
    *
    * @param bundleRegistry bundle registry for serializing ops, nodes, and custom types
    * @param fs file system for bundle
    * @param path path to the Bundle.ML directory
    * @tparam Context context for implementation
    * @return context for serializing Bundle.ML
    */
  def bundleContext[Context](context: Context,
                             bundleRegistry: BundleRegistry,
                             fs: FileSystem,
                             path: Path): BundleContext[Context] = {
    BundleContext[Context](context, info.format, bundleRegistry, fs, path)
  }
}
