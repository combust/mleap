package ml.combust.bundle

import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import ml.combust.bundle.fs.BundleFileSystem
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.ClassLoaderUtil

import scala.collection.JavaConverters._

/** Trait for classes that contain a bundle registry.
  *
  * This is used by methods that require access to a bundle registry.
  */
trait HasBundleRegistry {
  /** Get the bundle registry.
    *
    * @return bundle registry
    */
  def bundleRegistry: BundleRegistry
}

object BundleRegistry {
  def apply(registry: String): BundleRegistry = apply(registry, None)

  def apply(registry: String, cl: Option[ClassLoader]): BundleRegistry = {
    apply(registry, None, cl)
  }

  def apply(registry: String, configOption: Option[Config], clOption: Option[ClassLoader]): BundleRegistry = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[BundleRegistry].getCanonicalName))
    val config = configOption.getOrElse(ConfigFactory.load(cl))

    val registryConfig = config.getConfig(registry)
    val ops = registryConfig.getStringList("ops").asScala.flatMap {
      opsPath => config.getStringList(opsPath).asScala
    }

    val br = ops.foldLeft(Map[String, OpNode[_, _, _]]()) {
      (m, opClass) =>
        val opNode = cl.loadClass(opClass).newInstance().asInstanceOf[OpNode[_, _, _]]
        m + (opNode.Model.opName -> opNode)
    }.values.foldLeft(BundleRegistry(cl)) {
      (br, opNode) => br.register(opNode)
    }

    val fsConfigs = if (config.hasPath("file-systems")) {
      registryConfig.getConfigList("file-systems").asScala
    } else { Seq() }

    fsConfigs.foldLeft(br) {
      (br, fsConfig) =>
        val fs = cl.loadClass(fsConfig.getString("class")).
          getConstructor(classOf[Config]).
          newInstance(fsConfig).
          asInstanceOf[BundleFileSystem]
        br.registerFileSystem(fs)
    }
  }
}

/** Class for storing all supported [[ml.combust.bundle.op.OpNode]] objects.
  *
  * This is the primary registry for Bundle.ML. It contains all objects
  * required for serializing ML models, graph nodes and custom types.
  *
  * Many serialization calls in Bundle.ML require access to the registry for information
  * on how to serialize custom types or models or nodes.
  *
  * @param classLoader class loader used to create this registry
  */
case class BundleRegistry private (classLoader: ClassLoader) extends HasBundleRegistry {
  var ops: Map[String, OpNode[_, _, _]] = Map()
  var opAlias: Map[String, String] = Map()

  var models: Map[String, OpModel[_, _]] = Map()
  var modelAlias: Map[String, String] = Map()

  var fileSystems: Map[String, BundleFileSystem] = Map()

  override def bundleRegistry: BundleRegistry = this

  /** Get an op node by name.
    *
    * @param op name of op
    * @tparam Context context for implementation
    * @tparam N class of op
    * @tparam M model class of op
    * @return op node type class for given name
    */
  def apply[Context, N, M](op: String): OpNode[Context, N, M] = ops(op).asInstanceOf[OpNode[Context, N, M]]

  /** Get an op node for an object.
    *
    * @param obj node object
    * @tparam Context context for implementation
    * @tparam N class of op
    * @tparam M model class of op
    * @return op node type class for given object
    */
  def opForObj[Context, N, M](obj: N): OpNode[Context, N, M] = ops(opAlias(obj.getClass.getCanonicalName)).asInstanceOf[OpNode[Context, N, M]]

  /** Get a model for a name.
    *
    * @param op name of op
    * @tparam Context context for implementation
    * @tparam M model class of op
    * @return model type class for given name
    */
  def model[Context, M](op: String): OpModel[Context, M] = models(op).asInstanceOf[OpModel[Context, M]]

  /** Get a model for an object.
    *
    * @param obj model object
    * @tparam Context context for implementation
    * @tparam M model class
    * @return model type class for given object
    */
  def modelForObj[Context, M](obj: Any): OpModel[Context, M] = model(modelAlias(obj.getClass.getCanonicalName))

  /** Register an op node for serialization/deserialization.
    *
    * This will register the underlying model type class as well.
    *
    * @param op op node type class
    * @tparam Context context for implementation
    * @tparam N type of the op node
    * @tparam M type of the underlying model
    * @return this
    */
  def register[Context, N, M](op: OpNode[Context, N, M]): this.type = {
    ops = ops + (op.Model.opName -> op)
    opAlias = opAlias + (op.klazz.getCanonicalName -> op.Model.opName)
    models = models + (op.Model.opName -> op.Model)
    modelAlias = modelAlias + (op.Model.klazz.getCanonicalName -> op.Model.opName)
    this
  }

  /** Get the bundle file system for a URI.
    *
    * @param uri uri of the bundle in the file system
    * @return the file system
    */
  def fileSystemForUri(uri: URI): BundleFileSystem = {
    fileSystems(uri.getScheme)
  }

  /** Get the bundle file system for a URI.
    *
    * @param uri uri of the bundle in the file system
    * @return the file system
    */
  def getFileSystemForUri(uri: URI): Option[BundleFileSystem] = {
    fileSystems.get(uri.getScheme)
  }

  /** Register a file system for loading/saving bundles.
    *
    * @param fileSystem file system to register
    * @return this
    */
  def registerFileSystem(fileSystem: BundleFileSystem): this.type = {
    for (scheme <- fileSystem.schemes) { fileSystems += (scheme -> fileSystem) }
    this
  }
}
