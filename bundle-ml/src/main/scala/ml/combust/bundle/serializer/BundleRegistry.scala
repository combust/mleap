package ml.combust.bundle.serializer

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.custom.CustomType

import scala.reflect.{ClassTag, classTag}

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

/** Class for storing all supported [[ml.combust.bundle.op.OpNode]] and [[ml.combust.bundle.serializer.custom.CustomType]] objects.
  *
  * This is the primary registry for Bundle.ML. It contains all objects
  * required for serializing ML models, graph nodes and custom types.
  *
  * Many serialization calls in Bundle.ML require access to the registry for information
  * on how to serialize custom types or models or nodes.
  */
case class BundleRegistry() extends HasBundleRegistry {
  var ops: Map[String, OpNode[_, _]] = Map()
  var opAlias: Map[String, String] = Map()

  var models: Map[String, OpModel[_]] = Map()
  var modelAlias: Map[String, String] = Map()

  var customTypes: Map[String, CustomType[_]] = Map()
  var customTypeAlias: Map[String, String] = Map()

  override def bundleRegistry: BundleRegistry = this

  /** Get an op node by name.
    *
    * @param op name of op
    * @tparam N class of op
    * @tparam M model class of op
    * @return op node type class for given name
    */
  def apply[N, M](op: String): OpNode[N, M] = ops(op).asInstanceOf[OpNode[N, M]]

  /** Get an op node for an object.
    *
    * @param obj node object
    * @tparam N class of op
    * @tparam M model class of op
    * @return op node type class for given object
    */
  def opForObj[N, M](obj: N): OpNode[N, M] = ops(opAlias(obj.getClass.getCanonicalName)).asInstanceOf[OpNode[N, M]]

  /** Get a model for a name.
    *
    * @param op name of op
    * @tparam M model class of op
    * @return model type class for given name
    */
  def model[M](op: String): OpModel[M] = models(op).asInstanceOf[OpModel[M]]

  /** Get a model for an object.
    *
    * @param obj model object
    * @tparam M model class
    * @return model type class for given object
    */
  def modelForObj[M](obj: Any): OpModel[M] = model(modelAlias(obj.getClass.getCanonicalName))

  /** Get custom type by name.
    *
    * @param name name of custom object
    * @tparam T type of custom object
    * @return custom object type class
    */
  def custom[T](name: String): CustomType[T] = customTypes(name).asInstanceOf[CustomType[T]]

  /** Get custom type for an object.
    *
    * @param obj custom object
    * @tparam T type of custom object
    * @return custom object type class
    */
  def customForObj[T](obj: Any): CustomType[T] = custom(customTypeAlias(obj.getClass.getCanonicalName))

  /** Get custom type for a class.
    *
    * @tparam T type of custom object
    * @return custom object type class
    */
  def customForClass[T: ClassTag]: CustomType[T] = custom(customTypeAlias(classTag[T].runtimeClass.getCanonicalName)).asInstanceOf[CustomType[T]]

  /** Register an op node for serialization/deserialization.
    *
    * This will register the underlying model type class as well.
    *
    * @param op op node type class
    * @tparam N type of the op node
    * @tparam M type of the underlying model
    * @return this
    */
  def register[N: ClassTag, M: ClassTag](op: OpNode[N, M]): this.type = {
    ops = ops + (op.Model.opName -> op)
    opAlias = opAlias + (classTag[N].runtimeClass.getCanonicalName -> op.Model.opName)
    models = models + (op.Model.opName -> op.Model)
    modelAlias = modelAlias + (classTag[M].runtimeClass.getCanonicalName -> op.Model.opName)
    this
  }

  /** Register a custom type for serialization/deserialization.
    *
    * @param c custom type type class
    * @tparam T type of the custom type
    * @return this
    */
  def register[T: ClassTag](c: CustomType[T]): this.type = {
    customTypes = customTypes + (c.name -> c)
    customTypeAlias = customTypeAlias + (classTag[T].runtimeClass.getCanonicalName -> c.name)
    this
  }
}
