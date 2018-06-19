package ml.combust.mleap.springboot

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, StandardCopyOption}

import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}
import ml.combust.mleap.executor.{LoadModelRequest, ModelConfig}
import ml.combust.mleap.pb
import ml.combust.mleap.pb.Model
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.javadsl.LeapFrameBuilder
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameWriter}
import org.json4s.jackson.JsonMethods
import org.springframework.http.{HttpEntity, HttpHeaders}

import scala.concurrent.duration._
import scalapb.json4s.{Parser, Printer}

object TestUtil {

  lazy val demoUri = getClass.getClassLoader.getResource("demo.zip").toURI

  lazy val builder = new LeapFrameBuilder

  lazy val printer = new Printer(includingDefaultValueFields = true, formattingLongAsNumber = true)
  lazy val parser = new Parser()

  lazy val httpEntityWithProtoHeaders = new HttpEntity[Unit](protoHeaders)
  lazy val httpEntityWithJsonHeaders = new HttpEntity[Unit](jsonHeaders)

  lazy val protoHeaders = {
    val headers = new HttpHeaders
    headers.add("Content-Type", "application/x-protobuf")
    headers.add("X-Timeout", "2000")
    headers
  }
  lazy val jsonHeaders = {
    val headers = new HttpHeaders
    headers.add("Content-Type", "application/json")
    headers
  }

  private lazy val validFrame = DefaultLeapFrame(
    StructType(Seq(StructField("demo:a", ScalarType.Double),
      StructField("demo:c", ScalarType.Double),
      StructField("demo:d", ScalarType.Double))).get,
    Seq(Row(44.5, 22.1, 98.2)))

  lazy val protoLeapFrame = FrameWriter(validFrame, BuiltinFormats.binary).toBytes().get
  lazy val jsonLeapFrame = FrameWriter(validFrame, BuiltinFormats.json).toBytes().get

  lazy val incompleteLeapFrame = FrameWriter(DefaultLeapFrame(
      StructType(Seq(StructField("demo:a", ScalarType.Double),
      StructField("demo:d", ScalarType.Double))).get,
    Seq(Row(44.5, 98.2))),
    BuiltinFormats.binary).toBytes().get

  def buildLoadModelJsonRequest(modelName: String, uri:URI) = {
    val tmpFile: Path = createTempFile(uri)

    val loadModelRequest = LoadModelRequest(modelName = modelName,
                            uri = tmpFile.toUri,
                            config = ModelConfig(memoryTimeout = 15.minutes, diskTimeout = 15.minutes))

    val protoRequest = pb.LoadModelRequest(modelName = loadModelRequest.modelName,
      uri = loadModelRequest.uri.toString,
      config = Some(TypeConverters.executorToPbModelConfig(loadModelRequest.config)),
      force = loadModelRequest.force)

    JsonMethods.compact(printer.toJson(protoRequest))
  }

  private def createTempFile(uri: URI): Path = {
    val tmpFile = Files.createTempFile("demo", ".bundle.zip")
    val file = new File(uri.getPath).toPath
    Files.copy(file, tmpFile, StandardCopyOption.REPLACE_EXISTING)
    tmpFile.toFile.deleteOnExit()
    tmpFile
  }

  def extractModelResponse(response: String) = TypeConverters.pbToExecutorModel(parser.fromJsonString[Model](response))
}
