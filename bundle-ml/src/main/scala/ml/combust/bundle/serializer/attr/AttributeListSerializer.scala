package ml.combust.bundle.serializer.attr

import java.nio.file.{Files, Path}

import com.google.protobuf.CodedInputStream
import ml.combust.bundle.HasBundleRegistry
import ml.combust.bundle.json.JsonSupport._
import ml.combust.bundle.serializer.{SerializationContext, SerializationFormat}
import ml.combust.bundle.dsl.AttributeList
import spray.json._
import resource._

import scala.io.Source
import scala.util.Try

/** Class for serializing an [[ml.combust.bundle.dsl.AttributeList]].
  *
  * @param path path to base attribute list file (no extension)
  */
case class AttributeListSerializer(path: Path) {
  /** Write an attribute list to a file.
    *
    * Depending on the [[SerializationFormat]], the attribute list will
    * either be written to a protobuf file or a JSON file.
    *
    * @param attrs attribute list to write
    * @param context serialization context for determining format
    */
  def write(attrs: AttributeList)
           (implicit context: SerializationContext): Try[Any] = context.concrete match {
    case SerializationFormat.Json => writeJson(attrs)
    case SerializationFormat.Protobuf => writeProto(attrs)
  }

  /** Write attribute list as a JSON file.
    *
    * @param list attribute list to write
    * @param hr bundle registry for custom types
    */
  def writeJson(list: AttributeList)
               (implicit hr: HasBundleRegistry): Try[Any] = {
    val json = list.toJson.prettyPrint.getBytes
    (for(out <- managed(Files.newOutputStream(path))) yield {
      out.write(json)
    }).tried
  }

  /** Write attribute list as a Protobuf file.
    *
    * @param list attribute list to write
    * @param hr bundle registry for custom types
    */
  def writeProto(list: AttributeList)
                (implicit hr: HasBundleRegistry): Try[Any] = {
    (for(out <- managed(Files.newOutputStream(path))) yield {
      list.asBundle.writeTo(out)
    }).tried
  }

  /** Read an attribute list.
    *
    * [[SerializationFormat]] determines file name and format used for reading
    *
    * @param context serialization context used to determine format
    * @return attribute list deserialized from the appropriate file
    */
  def read()
          (implicit context: SerializationContext): Try[AttributeList] = context.concrete match {
    case SerializationFormat.Json => readJson()
    case SerializationFormat.Protobuf => readProto()
  }

  /** Read an attribute list from a JSON file.
    *
    * @param context serialization context for decoding custom values
    * @return attribute list from the JSON file
    */
  def readJson()
              (implicit context: SerializationContext): Try[AttributeList] = {
    Try(Files.readAllBytes(path)).map(bytes => new String(bytes)).map(_.parseJson.convertTo[AttributeList])
  }

  /** Read an attribute list from a Protobuf file.
    *
    * @param context serialization context for decoding custom values
    * @return attribute list from the protobuf file
    */
  def readProto()
               (implicit context: SerializationContext): Try[AttributeList] = {
    (for(in <- managed(Files.newInputStream(path))) yield {
      val cis = CodedInputStream.newInstance(in)
      cis.setSizeLimit(Integer.MAX_VALUE)
      AttributeList.fromBundle(ml.bundle.AttributeList.AttributeList.parseFrom(cis))
    }).tried
  }
}
