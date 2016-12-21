package ml.combust.bundle.dsl

import java.io.File

import ml.combust.mleap.BuildValues
import ml.combust.bundle.{BundleContext, BundleRegistry, HasBundleRegistry}
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
    }

    object feature {
      val binarizer = "binarizer"
      val string_indexer = "string_indexer"
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
      val elementwise_product = "elementwise_product"
      val normalizer = "normalizer"
      val pca = "pca"
      val ngram = "ngram"
      val polynomial_expansion = "polynomial_expansion"
      val stopwords_remover = "stopwords_remover"
    }

    object classification {
      val logistic_regression = "logistic_regression"
      val random_forest_classifier = "random_forest_classifier"
      val gbt_classifier = "gbt_classifier"
      val decision_tree_classifier = "decision_tree_classifier"
      val support_vector_machine = "support_vector_machine"
      val one_vs_rest = "one_vs_rest"
    }

    object clustering {
      val gaussian_mixture = "gaussian_mixture"
      val k_means = "k_means"
    }

    val pipeline = "pipeline"
  }

  /** Create a bundle.
    *
    * @param name name of bundle
    * @param format format of bundle
    * @param nodes nodes in bundle
    * @param attributes attributes of bundle
    * @return bundle
    */
  def createBundle(name: String,
                   format: SerializationFormat,
                   nodes: Seq[Any],
                   attributes: Option[AttributeList] = None): Bundle = {
    Bundle(name = name,
      format = format,
      version = Bundle.version,
      attributes = attributes, nodes)
  }
}

/** Meta data for a bundle.
  *
  * @param name name of the bundle
  * @param format serialization format of the [[Bundle]]
  * @param version Bundle.ML version used for serializing
  * @param attributes optional [[AttributeList]] to serialize with the bundle
  * @param nodes list of root nodes in the bundle
  */
case class BundleMeta(name: String,
                      format: SerializationFormat,
                      version: String,
                      attributes: Option[AttributeList],
                      nodes: Seq[String])

/** Root object for serializing Bundle.ML pipelines and graphs.
  *
  * @param name name of the bundle
  * @param format serialization format of the [[Bundle]]
  * @param version Bundle.ML version used for serializing
  * @param attributes optional [[AttributeList]] to serialize with the bundle
  * @param nodes list of root nodes in the bundle
  */
case class Bundle(name: String,
                  format: SerializationFormat,
                  version: String,
                  override val attributes: Option[AttributeList],
                  nodes: Seq[Any]) extends HasAttributeList[Bundle] {
  /** Create meta data for this bundle.
    *
    * @param hr bundle registry for custom types
    * @return bundle meta data
    */
  def meta(implicit hr: HasBundleRegistry): BundleMeta = {
    val nodeNames = nodes.map(node => hr.bundleRegistry.opForObj[Any, Any, Any](node).name(node))
    BundleMeta(name = name,
      format = format,
      version = version,
      attributes = attributes,
      nodes = nodeNames)
  }

  /** Create a [[BundleContext]] for serializing to Bundle.ML
    *
    * @param bundleRegistry bundle registry for serializing ops, nodes, and custom types
    * @param path path to the Bundle.ML directory
    * @tparam Context context for implementation
    * @return context for serializing Bundle.ML
    */
  def bundleContext[Context](context: Context,
                             bundleRegistry: BundleRegistry,
                             path: File): BundleContext[Context] = {
    BundleContext[Context](context, format, bundleRegistry, path)
  }

  override def replaceAttrList(list: Option[AttributeList]): Bundle = copy(attributes = list)
}
