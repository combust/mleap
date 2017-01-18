package ml.combust.bundle.serializer.attr

import ml.combust.bundle.HasBundleRegistry
import ml.combust.bundle.dsl.{Attribute, AttributeList}

/** Class to separate an [[ml.combust.bundle.dsl.AttributeList]] into two
  * [[ml.combust.bundle.dsl.AttributeList]] objects, one with small attributes and one with large.
  *
  * An [[ml.combust.bundle.dsl.Attribute]] is large or small according to the [[ml.combust.bundle.dsl.Value#isLarge]] and
  * [[ml.combust.bundle.dsl.Value#isSmall]] methods, respectively. This class is only used when
  * the [[ml.combust.bundle.serializer.SerializationFormat.Mixed]] mode of serialization is
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
  def separate(attributes: Option[AttributeList])
              (implicit hr: HasBundleRegistry): (Option[AttributeList], Option[AttributeList]) = attributes match {
    case None => (None, None)
    case Some(list) =>
      val (small, large) = list.lookup.foldLeft((Map[String, Attribute](), Map[String, Attribute]())) {
        case ((s, l), namedAttr) =>
          if(namedAttr._2.value.isSmall) {
            (s + namedAttr, l)
          } else {
            (s, l + namedAttr)
          }
      }

      (AttributeList.option(small), AttributeList.option(large))
  }
}
