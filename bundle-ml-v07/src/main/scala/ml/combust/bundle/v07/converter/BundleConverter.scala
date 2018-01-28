package ml.combust.bundle.v07.converter

import java.nio.file.{Files, Path}
import java.time.LocalDateTime

import ml.combust.bundle.dsl.{Bundle, BundleInfo}
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.bundle.v07

import ml.combust.bundle.v07.json.JsonSupport._
import ml.combust.bundle.json.JsonSupport._
import spray.json._
import scala.util.Try

/**
  * Created by hollinwilkins on 1/27/18.
  */
class BundleConverter {
  def convertFile(in: Path, out: Path): Try[Unit] = {
    val tryInfo = Try {
      new String(Files.readAllBytes(in), "UTF-8").
        parseJson.
        convertTo[ml.combust.bundle.v07.dsl.BundleInfo]
    }

    for (info <- tryInfo) yield {
      val newInfo = convert(info)
      Files.write(out, newInfo.asBundle.toJson.prettyPrint.getBytes)
    }
  }

  def convert(bundle: v07.dsl.BundleInfo)
             (implicit cc: ConverterContext): BundleInfo = {
    BundleInfo(uid = bundle.uid,
      name = bundle.name,
      version = Bundle.version,
      format = convertFormat(bundle.format),
      timestamp = LocalDateTime.now().toString)
  }

  def convertFormat(format: v07.dsl.SerializationFormat): SerializationFormat = format match {
    case v07.dsl.SerializationFormat.Json => SerializationFormat.Json
    case v07.dsl.SerializationFormat.Protobuf => SerializationFormat.Protobuf
    case v07.dsl.SerializationFormat.Mixed => SerializationFormat.Json
  }
}
