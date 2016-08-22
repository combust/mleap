package ml.bundle.dsl

import ml.bundle.ModelDef.ModelDef
import ml.bundle.serializer.SerializationContext

/** Trait for read-only operations on a [[Model]].
  *
  * Use this trait to expose a read-only interface to a [[Model]].
  * This is primarily used when deserializing.
  */
trait ReadableModel {
  /** Convert the [[Model]] to a protobuf serializable object.
    *
    * @param context serialization context for encoding custom values
    * @return protobuf model definition
    */
  def bundleModel(implicit context: SerializationContext): ModelDef

  /** Get the op name for this model.
    *
    * The op name defines what the model actually is. It corresponds
    * to an entry for a [[ml.bundle.op.OpNode]] in a [[ml.bundle.serializer.BundleRegistry]].
    *
    * [[ml.bundle.op.OpModel]] have all methods and data needed to serialize a model
    * for a given op name.
    *
    * Examples are "linear_regression", "string_indexer", "random_forest_classifier", etc.
    *
    * @return op name
    */
  def op: String

  /** Get list of attributes for this model.
    *
    * @return optional list read-only attribute list
    */
  def attributes: Option[ReadableAttributeList]

  /** Get a specific attribute by name.
    *
    * Throws an error if the attribute does not exist.
    *
    * @param name name of the attribute
    * @return the attribute
    */
  def attr(name: String): Attribute

  /** Get an optional attribute by name.
    *
    * @param name name of the attribute
    * @return an optional attribute
    */
  def getAttr(name: String): Option[Attribute]

  /** Get the value of an attribute.
    *
    * Throws an error if the attribute does not exist.
    *
    * @param name name of the attribute
    * @return value of the attribute
    */
  def value(name: String): Value

  /** Get an optional value for an attribute.
    *
    * @param name name of the attribute
    * @return optional value of the attribute
    */
  def getValue(name: String): Option[Value]
}

/** Trait for a writable interface to a [[Model]].
  *
  * Use this trait during serialization of [[Model]] objects.
  */
trait WritableModel extends ReadableModel {
  /** Add an attribute to the model.
    *
    * @param attribute attribute to add
    * @return copy of the model with attribute appended
    */
  def withAttr(attribute: Attribute): WritableModel

  /** Add attributes from an attribute list to the model.
    *
    * @param list list of attributes to add
    * @return copy of the model with all attributes appended
    */
  def withAttrList(list: AttributeList): WritableModel

  /** Replace all attributes with another list.
    *
    * @param list list of attributes used to replace existing attributes
    * @return copy of model with attributes set to the given list
    */
  def replaceAttrList(list: AttributeList): WritableModel
}

/** Class that encodes all information need to serialize or deserialize
  * a machine learning model.
  *
  * Models encode things like coefficients for linear regressions,
  * labels for string indexers, sizes for one hot encoders, decision trees,
  * and any other data needed to serialize and deserialize ML models and
  * feature builders.
  *
  * Usually you will want to pass [[ReadableModel]] or [[WritableModel]] to
  * a user depending on the use case to constrict the allowed operations.
  *
  * @param op op name for the model
  * @param attributes optional list of attributes for the model
  */
case class Model(override val op: String,
                 override val attributes: Option[AttributeList] = None) extends WritableModel
  with HasAttributeList[Model] {
  override def bundleModel(implicit context: SerializationContext): ModelDef = {
    ModelDef(op = op,
      attributes = attributes.map(_.bundleList))
  }

  override def replaceAttrList(list: AttributeList): Model = {
    copy(attributes = Some(list))
  }
}
