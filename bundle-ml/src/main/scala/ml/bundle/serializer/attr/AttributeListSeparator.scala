package ml.bundle.serializer.attr

import ml.bundle.serializer.HasBundleRegistry
import ml.bundle.dsl.{Attribute, AttributeList, ReadableAttributeList}

/** Class to separate an [[ml.bundle.dsl.AttributeList]] into two
  * [[AttributeList]] objects, one with small attributes and one with large.
  *
  * An [[Attribute]] is large or small according to the [[ml.bundle.dsl.Value#isLarge]] and
  * [[ml.bundle.dsl.Value#isSmall]] methods, respectively. This class is only used when
  * the [[ml.bundle.serializer.SerializationFormat.Mixed]] mode of serialization is
  * being used, and large attributes are intended to go into a protobuf file, while
  * small attributes go into a JSON file along with the model contents.
  */
case class AttributeListSeparator() {
  /** Separate an attribute list into a small/large list.
    *
    * @param attributes optional list of attributes
    * @param hr bundle registry for determining small or large for custom attributes
    * @return an optional small and large attribute list
    */
  def separate(attributes: Option[ReadableAttributeList])
              (implicit hr: HasBundleRegistry): (Option[AttributeList], Option[AttributeList]) = attributes match {
    case None => (None, None)
    case Some(list) =>
      val (small, large) = list.attributes.foldLeft((Seq[Attribute](), Seq[Attribute]())) {
        case ((s, l), attr) =>
          if(attr.value.isSmall) {
            (s :+ attr, l)
          } else {
            (s, l :+ attr)
          }
      }

      (AttributeList.option(small), AttributeList.option(large))
  }
}
