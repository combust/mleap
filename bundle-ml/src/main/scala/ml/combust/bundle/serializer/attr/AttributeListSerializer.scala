package ml.combust.bundle.serializer.attr

import java.io.{File, FileInputStream, FileOutputStream}

import ml.combust.bundle.json.JsonSupport._
import ml.combust.bundle.serializer.{HasBundleRegistry, SerializationContext, SerializationFormat}
import ml.combust.bundle.dsl.AttributeList
import spray.json._
import resource._

import scala.io.Source

/** Class for serializing an [[ml.combust.bundle.dsl.AttributeList]].
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
    * @param hr bundle registry for custom types
    */
  def writeJson(list: AttributeList)
               (implicit hr: HasBundleRegistry): Unit = {
    val json = list.toJson.prettyPrint.getBytes
    for(out <- managed(new FileOutputStream(path))) {
      out.write(json)
    }
  }

  /** Write attribute list as a Protobuf file.
    *
    * @param list attribute list to write
    * @param hr bundle registry for custom types
    */
  def writeProto(list: AttributeList)
                (implicit hr: HasBundleRegistry): Unit = {
    for(out <- managed(new FileOutputStream(path))) {
      list.asBundle.writeTo(out)
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
      Source.fromInputStream(in).getLines().mkString.parseJson.convertTo[AttributeList]
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(list) => list
    }
  }

  /** Read an attribute list from a Protobuf file.
    *
    * @param context serialization context for decoding custom values
    * @return attribute list from the protobuf file
    */
  def readProto()
               (implicit context: SerializationContext): AttributeList = {
    (for(in <- managed(new FileInputStream(path))) yield {
      AttributeList.fromBundle(ml.bundle.AttributeList.AttributeList.parseFrom(in))
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(list) => list
    }
  }
}
