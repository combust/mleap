package ml.combust.mleap.springboot

import java.net.URI

import com.google.protobuf.ByteString
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.http._
import ml.combust.mleap.pb._
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameWriter}
import org.json4s.jackson.JsonMethods

import scalapb.json4s.{Parser, Printer}

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = RANDOM_PORT)
class JsonScoringControllerSpec extends ScoringBase[String, String, String, String, String] {

  lazy val printer = new Printer(includingDefaultValueFields = true, formattingLongAsNumber = true)
  lazy val parser = new Parser()

  override def createLoadModelRequest(modelName: String, uri:URI, createTmpFile: Boolean): HttpEntity[String] = {
    val request = LoadModelRequest(modelName = modelName,
      uri = TestUtil.getBundle(uri, createTmpFile).toString,
      config = Some(ModelConfig(900L, 900L)))

    new HttpEntity[String](JsonMethods.compact(printer.toJson(request)), JsonScoringControllerSpec.jsonHeaders)
  }

  override def createTransformFrameRequest(modelName: String, frame: DefaultLeapFrame): HttpEntity[String] = {
    val request = TransformFrameRequest(modelName = modelName,
      format = BuiltinFormats.json,
      initTimeout = 35000L,
      frame = ByteString.copyFrom(FrameWriter(frame, BuiltinFormats.json).toBytes().get),
      options = None
    )
    new HttpEntity[String](JsonMethods.compact(printer.toJson(request)), JsonScoringControllerSpec.jsonHeaders)
  }

  override def extractModelResponse(response: ResponseEntity[_ <: Any]): Mleap.Model =
    Model.toJavaProto(parser.fromJsonString[Model](response.getBody.asInstanceOf[String]))

  override def createEmptyBodyRequest(): HttpEntity[Unit] = JsonScoringControllerSpec.httpEntityWithJsonHeaders

  override def extractBundleMetaResponse(response: ResponseEntity[_]): Mleap.BundleMeta =
    BundleMeta.toJavaProto(parser.fromJsonString[BundleMeta](response.getBody.asInstanceOf[String]))

  override def extractTransformResponse(response: ResponseEntity[_]): Mleap.TransformFrameResponse =
    TransformFrameResponse.toJavaProto(parser.fromJsonString[TransformFrameResponse](response.getBody.asInstanceOf[String]))

  override def leapFrameFormat(): String = BuiltinFormats.json

  describe("json scoring controller - load model endpoint") {

    it("returns BAD_REQUEST error with empty request") {
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String]("", JsonScoringControllerSpec.jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("returns BAD_REQUEST error when request is not valid json") {
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String]("{invalid json}", JsonScoringControllerSpec.jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }
  }
}

object JsonScoringControllerSpec {
  lazy val httpEntityWithJsonHeaders = new HttpEntity[Unit](jsonHeaders)

  lazy val jsonHeaders = {
    val headers = new HttpHeaders
    headers.add("Content-Type", "application/json")
    headers.add("timeout", "2000")
    headers
  }
}
