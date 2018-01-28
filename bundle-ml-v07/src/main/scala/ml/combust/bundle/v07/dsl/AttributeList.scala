package ml.combust.bundle.v07.dsl

/** This trait provides easy access to reading/writing attributes
  * to objects that contain an [[AttributeList]].
  *
  * @tparam T class type that contains the attribute list
  */
trait HasAttributeList[T] {
  /** The attribute list */
  def attributes: Option[AttributeList]

  /** Get an attribute.
    *
    * Throws an error if it does not exist.
    *
    * @param name name of the attribute
    * @return the attribute
    */
  def attr(name: String): Attribute = attributes.get(name)

  /** Get an attribute as an option.
    *
    * @param name name of the attribute
    * @return Some(attribute) or None if it is not in the list
    */
  def getAttr(name: String): Option[Attribute] = attributes.get.get(name)

  /** Get the [[Value]] of an attribute.
    *
    * Throws an error if that attribute does not exist.
    *
    * @param name name of the attribute
    * @return [[Value]] of the attribute
    */
  def value(name: String): Value = attr(name).value

  /** Get the [[Value]] of an attribute as an Option.
    *
    * @param name name of the attribute
    * @return [[Value]] of the attribute as an Option[Value], None if it does not exist
    */
  def getValue(name: String): Option[Value] = getAttr(name).map(_.value)
}

/** Companion class for construction and conversion of [[AttributeList]] objects.
  */
object AttributeList {
  /** Construct an [[AttributeList]] from a list of [[Attribute]].
    *
    * @param attrs list of [[Attribute]]
    * @return [[AttributeList]] with all attrs passed in
    */
  def apply(attrs: Seq[(String, Attribute)]): AttributeList = {
    AttributeList(attrs.toMap)
  }

  /** Construct an [[AttributeList]] from a list of [[Attribute]].
    *
    * @param attr1 first [[Attribute]] in list
    * @param attrs tail of [[Attribute]] list
    * @return [[AttributeList]] with attr1 and attrs in the list
    */
  def apply(attr1: (String, Attribute), attrs: (String, Attribute) *): AttributeList = {
    AttributeList(Seq(attr1) ++ attrs)
  }

  /** Create an attribute list from a bundle attribute list.
    *
    * @param list bundle attribute list
    * @return dsl attribute list
    */
  def fromBundle(list: ml.bundle.v07.AttributeList): AttributeList = {
    val attrs = list.attributes.map {
      case (key, attr) => (key, Attribute.fromBundle(attr))
    }
    AttributeList(attrs)
  }

  /** Construct an optional [[AttributeList]].
    *
    * Returns None if there are no attributes in the list.
    *
    * @param attrs [[Attribute]] in the list
    * @return None if {{{attrs.isEmpty}}} otherwise Some(attributeList)
    */
  def option(attrs: Map[String, Attribute]): Option[AttributeList] = {
    if(attrs.nonEmpty) { Some(apply(attrs)) } else { None }
  }
}

/** Class that holds a list of [[Attribute]] objects.
  *
  * Can only have one attribute with a given name at a time.
  *
  * @param lookup map of attribute name to [[Attribute]] object
  */
case class AttributeList(lookup: Map[String, Attribute]) {
  /* make sure the list is not empty */
  require(lookup.nonEmpty, "attribute list cannot be empty")

  /** Get an attribute.
    *
    * Alias for [[attr]]
    *
    * @param name name of the attribute
    * @return the attribute
    */
  def apply(name: String): Attribute = attr(name)

  /** Get an [[Attribute]] from the list.
    *
    * Throws an error if the attribute doesn't exist.
    *
    * @param name name of the attribute
    * @return the attribute
    */
  def attr(name: String): Attribute = lookup(name)

  /** Get an option of an [[Attribute]] from the list.
    *
    * @param name name of the attribute
    * @return optional [[Attribute]] from list
    */
  def get(name: String): Option[Attribute] = lookup.get(name)
}
