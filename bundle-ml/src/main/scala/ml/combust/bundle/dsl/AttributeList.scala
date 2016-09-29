package ml.combust.bundle.dsl

import ml.combust.bundle.serializer.SerializationContext

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

  /** Add an attribute to the list.
    *
    * @param attribute attribute to add to the list
    * @return a copy of T with the attribute added to the attribute list
    */
  def withAttr(attribute: Attribute): T = attributes match {
    case Some(l) => replaceAttrList(l.withAttr(attribute))
    case None => replaceAttrList(AttributeList(attribute))
  }

  /** Add a list of attributes to [[attributes]].
    *
    * Adds all attributes in the list argument to [[attributes]].
    *
    * @param list list of attributes to add
    * @return copy of T with all attributes added to [[attributes]]
    */
  def withAttrList(list: AttributeList): T = attributes match {
    case Some(l) => replaceAttrList(l.withAttrList(list))
    case None => replaceAttrList(list)
  }

  /** Replace the [[attributes]] with another list.
    *
    * @param list attributes to use to replace
    * @return copy of T with [[attributes]] replaced by list arg
    */
  def replaceAttrList(list: AttributeList): T
}

/** Trait for read-only operations of an [[AttributeList]].
  *
  * Use this trait when you only want to grant read-access to an [[AttributeList]].
  * This is used when deserializing [[Model]] or [[Bundle]] objects.
  *
  */
trait ReadableAttributeList {
  /** Get an attribute.
    *
    * Alias for [[attr]]
    *
    * @param name name of the attribute
    * @return the attribute
    */
  def apply(name: String): Attribute = attr(name)

  /** Convert to a protobuf attribute list
    *
    * @param context serialization context for encoding custom values
    * @return protobuf attribute list
    */
  def bundleList(implicit context: SerializationContext): ml.bundle.AttributeList.AttributeList

  /** Iteratable of all [[Attribute]] in the list.
    *
    * @return iterable of all [[Attribute]] in list
    */
  def attributes: Iterable[Attribute]

  /** Get an [[Attribute]] from the list.
    *
    * Throws an error if the attribute doesn't exist.
    *
    * @param name name of the attribute
    * @return the attribute
    */
  def attr(name: String): Attribute

  /** Get an option of an [[Attribute]] from the list.
    *
    * @param name name of the attribute
    * @return optional [[Attribute]] from list
    */
  def get(name: String): Option[Attribute]
}

/** Trait for write operations on an [[AttributeList]].
  *
  * Use this trait when you need to provide a writable interface to the [[AttributeList]].
  * This is mainly used when serializing [[Model]] or [[Bundle]] objects.
  */
trait WritableAttributeList extends ReadableAttributeList {
  /** Add an attribute to the list.
    *
    * @param attribute attribute to add
    * @return copy of [[AttributeList]] with attribute added
    */
  def withAttr(attribute: Attribute): WritableAttributeList

  /** Add a list of attributes to the list.
    *
    * @param list list of attributes to add
    * @return copy of [[AttributeList]] with all attributes added
    */
  def withAttrList(list: ReadableAttributeList): WritableAttributeList
}

/** Companion class for construction and conversion of [[AttributeList]] objects.
  */
object AttributeList {
  /** Construct an [[AttributeList]] from a protobuf attribute list.
    *
    * @param list protobuf attribute list
    * @param context serialization context for decoding custom values
    * @return [[AttributeList]] of all attributes in the list param
    */
  def apply(list: ml.bundle.AttributeList.AttributeList)
           (implicit context: SerializationContext): AttributeList = {
    apply(list.attributes)
  }

  /** Construct an [[AttributeList]] from a list of protobuf attributes.
    *
    * @param attrs list of protobuf attributes
    * @param context serialization context for decoding custom values
    * @return [[AttributeList]] of all attributes in the list
    */
  def apply(attrs: Seq[ml.bundle.Attribute.Attribute])
           (implicit context: SerializationContext): AttributeList = {
    val lookup = attrs.map(a => (a.name, Attribute(a))).toMap
    AttributeList(lookup)
  }

  /** Construct an [[AttributeList]] from a list of [[Attribute]].
    *
    * @param attrs list of [[Attribute]]
    * @return [[AttributeList]] with all attrs passed in
    */
  def apply(attrs: Seq[Attribute]): AttributeList = {
    AttributeList(attrs.map(a => (a.name, a)).toMap)
  }

  /** Construct an [[AttributeList]] from a list of [[Attribute]].
    *
    * @param attr1 first [[Attribute]] in list
    * @param attrs tail of [[Attribute]] list
    * @return [[AttributeList]] with attr1 and attrs in the list
    */
  def apply(attr1: Attribute, attrs: Attribute *): AttributeList = {
    AttributeList(Seq(attr1) ++ attrs)
  }

  /** Construct an optional [[AttributeList]].
    *
    * Returns None if there are no attributes in the list.
    *
    * @param attrs [[Attribute]] in the list
    * @return None if {{{attrs.isEmpty}}} otherwise Some(attributeList)
    */
  def option(attrs: Seq[Attribute]): Option[AttributeList] = {
    if(attrs.nonEmpty) { Some(apply(attrs)) } else { None }
  }
}

/** Class that holds a list of [[Attribute]] objects.
  *
  * Can only have one attribute with a given name at a time.
  *
  * @param lookup map of attribute name to [[Attribute]] object
  */
case class AttributeList(lookup: Map[String, Attribute]) extends WritableAttributeList {
  /* make sure the list is not empty */
  if(lookup.isEmpty) { throw new Error("cannot have empty attribute list") } // TODO: better error

  override def bundleList(implicit context: SerializationContext): ml.bundle.AttributeList.AttributeList = {
    ml.bundle.AttributeList.AttributeList(lookup.values.map(a => a.bundleAttribute).toSeq)
  }

  override def attributes: Iterable[Attribute] = lookup.values
  override def attr(name: String): Attribute = lookup(name)
  override def get(name: String): Option[Attribute] = lookup.get(name)

  override def withAttr(attribute: Attribute): AttributeList = {
    copy(lookup = lookup + (attribute.name -> attribute))
  }

  override def withAttrList(list: ReadableAttributeList): AttributeList = {
    list.attributes.foldLeft(this) {
      case (l, attr) => l.withAttr(attr)
    }
  }
}
