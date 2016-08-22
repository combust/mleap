package ml.bundle.serializer.attr

import java.io.{File, FileInputStream, FileOutputStream}

import ml.bundle.json.JsonSupport._
import ml.bundle.serializer.{HasBundleRegistry, SerializationContext, SerializationFormat}
import ml.bundle.dsl.AttributeList
import spray.json._
import resource._

import scala.io.Source

/** Class for serializing an [[ml.bundle.dsl.AttributeList]].
  *
  * @param path path to base attribute list file (no extension)
  */
case class AttributeListSerializer(path: File) {
  /** Write an attribute list to a file.
    *
    * Depending on the [[SerializationFormat]], the attribute list will
    * either be written to a protobuf file or a JSON file.
    *
    * @param attrs attribute list to write
    * @param context serialization context for determining format
    */
  def write(attrs: AttributeList)
           (implicit context: SerializationContext): Unit = context.concrete match {
    case SerializationFormat.Json => writeJson(attrs)
    case SerializationFormat.Protobuf => writeProto(attrs)
  }

  /** Write attribute list as a JSON file.
    *
    * @param list attribute list to write
    * @param context serialization context for encoding custom values
    */
  def writeJson(list: AttributeList)
               (implicit context: SerializationContext): Unit = {
    val json = list.bundleList.toJson.prettyPrint.getBytes
    for(out <- managed(new FileOutputStream(path))) {
      out.write(json)
    }
  }

  /** Write attribute list as a Protobuf file.
    *
    * @param list attribute list to write
    * @param context serialization context for encoding custom values
    */
  def writeProto(list: AttributeList)
                (implicit context: SerializationContext): Unit = {
    for(out <- managed(new FileOutputStream(path))) {
      list.bundleList.writeTo(out)
    }
  }

  /** Read an attribute list.
    *
    * [[SerializationFormat]] determines file name and format used for reading
    *
    * @param context serialization context used to determine format
    * @return attribute list deserialized from the appropriate file
    */
  def read()
          (implicit context: SerializationContext): AttributeList = context.concrete match {
    case SerializationFormat.Json => readJson()
    case SerializationFormat.Protobuf => readProto()
  }

  /** Read an attribute list from a JSON file.
    *
    * @param context serialization context for decoding custom values
    * @return attribute list from the JSON file
    */
  def readJson()
              (implicit context: SerializationContext): AttributeList = {
    (for(in <- managed(new FileInputStream(path))) yield {
      val json = Source.fromInputStream(in).getLines().mkString
      AttributeList(json.parseJson.convertTo[ml.bundle.AttributeList.AttributeList])
    }).opt.get
  }

  /** Read an attribute list from a Protobuf file.
    *
    * @param context serialization context for decoding custom values
    * @return attribut elist from the protobuf file
    */
  def readProto()
               (implicit context: SerializationContext): AttributeList = {
    (for(in <- managed(new FileInputStream(path))) yield {
      AttributeList(ml.bundle.AttributeList.AttributeList.parseFrom(in))
    }).opt.get
  }
}
