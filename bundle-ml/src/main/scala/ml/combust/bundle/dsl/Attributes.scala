package ml.combust.bundle.dsl

/** This trait provides easy access to reading/writing attributes
  * to objects that contain an [[Attributes]].
  *
  * @tparam T class type that contains the attribute list
  */
trait HasAttributes[T] {
  /** The attribute list */
  def attributes: Attributes

  /** Get the [[Value]] of an attribute.
    *
    * Throws an error if that attribute does not exist.
    *
    * @param name name of the attribute
    * @return [[Value]] of the attribute
    */
  def value(name: String): Value = attributes(name)

  /** Get the [[Value]] of an attribute as an Option.
    *
    * @param name name of the attribute
    * @return [[Value]] of the attribute as an Option[Value], None if it does not exist
    */
  def getValue(name: String): Option[Value] = attributes.get(name)

  /** Add an attribute to the list.
    *
    * @param name name of the attribute
    * @param value value of the attribute
    * @return a copy of T with the attribute added
    */
  def withValue(name: String, value: Value): T = withAttributes(attributes.withValue(name, value))

  /** Add an optional attribute to the list.
    *
    * @param name name of the attribute
    * @param value optional value
    * @return a copy of T with the attribute optionally added
    */
  def withValue(name: String, value: Option[Value]): T = value.map {
    v => withAttributes(attributes.withValue(name, v))
  }.getOrElse(withAttributes(attributes))

  /** Replace the [[attributes]] with another list.
    *
    * @param attrs attributes to use to replace
    * @return copy of T with [[attributes]] replaced by list arg
    */
  def withAttributes(attrs: Attributes): T
}

/** Companion class for construction and conversion of [[Attributes]] objects.
  */
object Attributes {
  /** Construct an empty [[Attributes]].
    *
    * @return empty [[Attributes]]
    */
  def apply(): Attributes = new Attributes(Map())

  /** Construct an [[Attributes]] from a list of [[(String, Value)]].
    *
    * @param attrs list of [[(String, Value)]]
    * @return [[Attributes]] with all attrs passed in
    */
  def apply(attrs: Seq[(String, Value)]): Attributes = {
    Attributes(attrs.toMap)
  }

  /** Construct an [[Attributes]] from a list of [[(String, Value)]].
    *
    * @param attr1 first [[(String, Value)]] in list
    * @param attrs tail of [[(String, Value)]] list
    * @return [[Attributes]] with attr1 and attrs in the list
    */
  def apply(attr1: (String, Value), attrs: (String, Value) *): Attributes = {
    Attributes(Seq(attr1) ++ attrs)
  }

  /** Create an attribute list from a bundle attribute list.
    *
    * @param attrs bundle attributes
    * @return dsl attribute list
    */
  def fromBundle(attrs: ml.bundle.bundle.Attributes): Attributes = {
    val a = attrs.list.map {
      case (key, value) => (key, Value(value))
    }
    Attributes(a)
  }
}

/** Class that holds a map of string to [[ml.combust.bundle.dsl.Value]] objects.
  *
  * Can only have one attribute with a given name at a time.
  *
  * @param lookup map of attribute name to [[ml.combust.bundle.dsl.Value]] object
  */
case class Attributes(lookup: Map[String, Value]) {
  /** Convert to bundle attribute list.
    *
    * @return bundle attribute list
    */
  def asBundle: ml.bundle.bundle.Attributes = {
    val attrs = lookup.map {
      case (key, value) => (key, value.value)
    }

    ml.bundle.bundle.Attributes(attrs)
  }

  /** Get an attribute.
    *
    * Alias for [[value]]
    *
    * @param name name of the attribute
    * @return the attribute
    */
  def apply(name: String): Value = value(name)

  /** Get a [[Value]] from the list.
    *
    * Throws an error if the attribute doesn't exist.
    *
    * @param name name of the attribute
    * @return the attribute
    */
  def value(name: String): Value = lookup(name)

  /** Get an option of an [[Value]] from the list.
    *
    * @param name name of the attribute
    * @return optional [[Value]] from list
    */
  def get(name: String): Option[Value] = lookup.get(name)

  /** Add an attribute to the list.
    *
    * @param name name of attribute
    * @param value value to add
    * @return copy of [[Attributes]] with attribute added
    */
  def withValue(name: String, value: Value): Attributes = {
    copy(lookup = lookup + (name -> value))
  }
}
