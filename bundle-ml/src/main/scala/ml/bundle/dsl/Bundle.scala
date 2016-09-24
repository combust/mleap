package ml.bundle.dsl

import java.io.File

import ml.bundle.serializer.{BundleContext, BundleRegistry, SerializationContext, SerializationFormat}
import ml.bundle.{BundleDef, dsl}

/** Companion class for constants and constructors of [[Bundle]] objects.
  *
  * Contains file names for bundle JSON files and model JSON files.
  */
object Bundle {
  val version = "0.2.0"
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
    }

    object feature {
      val string_indexer = "string_indexer"
      val reverse_string_indexer = "reverse_string_indexer"
      val hashing_term_frequency = "hashing_term_frequency"
      val standard_scaler = "standard_scaler"
      val tokenizer = "tokenizer"
      val vector_assembler = "vector_assembler"
      val one_hot_encoder = "one_hot_encoder"
      val min_max_scaler = "min_max_scaler"
      val max_abs_scaler = "max_abs_scaler"
      val bucketizer = "bucketizer"
      val elementwise_product = "elementwise_product"
      val normalizer = "normalizer"
    }

    object classification {
      val logistic_regression = "logistic_regression"
      val random_forest_classifier = "random_forest_classifier"
      val decision_tree_classifier = "decision_tree_classifier"
      val support_vector_machine = "support_vector_machine"
      val one_vs_rest = "one_vs_rest"
    }

    val pipeline = "pipeline"
  }

  def createBundle(name: String,
                   format: SerializationFormat,
                   nodes: Seq[Any],
                   attributes: Option[dsl.AttributeList] = None): Bundle = {
    Bundle(name = name,
      format = format,
      version = Bundle.version,
      attributes = attributes, nodes)
  }
}

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
                  attributes: Option[dsl.AttributeList],
                  nodes: Seq[Any]) extends HasAttributeList[Bundle] {
  /** Convert to protobuf bundle definition.
    *
    * @param context bundle context for serialization format
    * @param sc serialization context for decoding custom values
    * @return protobuf bundle definition
    */
  def bundleDef(context: BundleContext)
               (implicit sc: SerializationContext): BundleDef.BundleDef = {
    BundleDef.BundleDef(name = name,
      format = format.bundleFormat,
      version = version,
      attributes = attributes.map(_.bundleList.attributes).map(ml.bundle.AttributeList.AttributeList.apply))
  }

  /** Create a [[ml.bundle.serializer.BundleContext]] for serializing to Bundle.ML
    *
    * @param bundleRegistry bundle registry for serializing ops, nodes, and custom types
    * @param path path to the Bundle.ML directory
    * @return context for serializing Bundle.ML
    */
  def bundleContext(bundleRegistry: BundleRegistry,
                    path: File): BundleContext = BundleContext(this.format, bundleRegistry, path)

  override def replaceAttrList(list: dsl.AttributeList): Bundle = copy(attributes = Some(list))
}
