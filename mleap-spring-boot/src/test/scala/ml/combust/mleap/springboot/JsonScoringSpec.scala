package ml.combust.mleap.springboot

import java.net.URI
import java.util.UUID
import com.google.protobuf.ByteString
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.http._
import ml.combust.mleap.pb._
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameWriter}
import ml.combust.mleap.springboot.TestUtil.{demoUri, validFrame}
import org.json4s.jackson.JsonMethods
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.test.context.junit.jupiter.SpringExtension
import scalapb.json4s.{Parser, Printer}

@ExtendWith(Array(classOf[SpringExtension]))
@SpringBootTest(webEnvironment = RANDOM_PORT)
class JsonScoringSpec extends ScoringBase[String, String, String, String, String] {

  lazy val printer = new Printer(includingDefaultValueFields = true, formattingLongAsNumber = true)
  lazy val parser = new Parser()

  override def createLoadModelRequest(modelName: String, uri:URI, createTmpFile: Boolean): HttpEntity[String] = {
    val request = LoadModelRequest(modelName = modelName,
      uri = TestUtil.getBundle(uri, createTmpFile).toString,
      config = Some(ModelConfig(Some(9000L), Some(9000L))))

    new HttpEntity[String](JsonMethods.compact(printer.toJson(request)), JsonScoringSpec.jsonHeaders)
  }

  override def createTransformFrameRequest(modelName: String, frame: DefaultLeapFrame, options: Option[TransformOptions]): HttpEntity[String] = {
    val request = TransformFrameRequest(modelName = modelName,
      format = BuiltinFormats.json,
      initTimeout = Some(35000L),
      frame = ByteString.copyFrom(FrameWriter(frame, BuiltinFormats.json).toBytes().get),
      options = options
    )
    new HttpEntity[String](JsonMethods.compact(printer.toJson(request)), JsonScoringSpec.jsonHeaders)
  }

  override def createTransformFrameRequest(frame: DefaultLeapFrame): HttpEntity[Array[Byte]] = {
    new HttpEntity[Array[Byte]](FrameWriter(validFrame, leapFrameFormat()).toBytes().get, JsonScoringSpec.jsonHeaders)
  }

  override def extractModelResponse(response: ResponseEntity[_ <: Any]): Mleap.Model =
    Model.toJavaProto(parser.fromJsonString[Model](response.getBody.asInstanceOf[String]))

  override def createEmptyBodyRequest(): HttpEntity[Unit] = JsonScoringSpec.httpEntityWithJsonHeaders

  override def extractBundleMetaResponse(response: ResponseEntity[_]): Mleap.BundleMeta =
    BundleMeta.toJavaProto(parser.fromJsonString[BundleMeta](response.getBody.asInstanceOf[String]))

  override def extractTransformResponse(response: ResponseEntity[_]): Mleap.TransformFrameResponse =
    TransformFrameResponse.toJavaProto(parser.fromJsonString[TransformFrameResponse](response.getBody.asInstanceOf[String]))

  override def createInvalidTransformFrameRequest(modelName: String, bytes: Array[Byte]): HttpEntity[String] = {
    val request = TransformFrameRequest(modelName = modelName,
      format = BuiltinFormats.json,
      initTimeout = Some(35000L),
      frame = ByteString.copyFrom(bytes),
      options = None
    )
    new HttpEntity[String](JsonMethods.compact(printer.toJson(request)), JsonScoringSpec.jsonHeaders)
  }


  override def leapFrameFormat(): String = BuiltinFormats.json

  describe("json scoring controller - load model endpoint") {

    it("returns BAD_REQUEST error with empty request") {
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String]("", JsonScoringSpec.jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("returns BAD_REQUEST error when request is not valid json") {
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String]("{invalid json}", JsonScoringSpec.jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }
  }

  describe("json scoring controller - transform endpoint") {
    it("returns BAD_REQUEST error with empty request") {
      val modelName = UUID.randomUUID().toString
      val loadModelRequest = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest, classOf[String])

      waitUntilModelLoaded(modelName, 10)

      val response = restTemplate.exchange("/models/transform", HttpMethod.POST,
        new HttpEntity[String]("", JsonScoringSpec.jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("returns BAD_REQUEST error when request is not valid json") {
      val modelName = UUID.randomUUID().toString
      val loadModelRequest = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest, classOf[String])

      waitUntilModelLoaded(modelName, 10)

      val response = restTemplate.exchange("/models/transform", HttpMethod.POST,
        new HttpEntity[String]("{invalid json}", JsonScoringSpec.jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }
  }
}

object JsonScoringSpec {
  lazy val httpEntityWithJsonHeaders = new HttpEntity[Unit](jsonHeaders)

  lazy val jsonHeaders = {
    val headers = new HttpHeaders
    headers.add("Content-Type", "application/json")
    headers.add("timeout", "2000")
    headers
  }
}
