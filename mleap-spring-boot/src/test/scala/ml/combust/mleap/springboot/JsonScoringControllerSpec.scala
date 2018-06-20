package ml.combust.mleap.springboot

import java.net.URI

import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.http._
import TestUtil._
import ml.combust.mleap.executor.{LoadModelRequest, ModelConfig}
import ml.combust.mleap.pb
import ml.combust.mleap.pb.{BundleMeta, Mleap, Model}
import org.json4s.jackson.JsonMethods

import scala.concurrent.duration._
import scalapb.json4s.{Parser, Printer}

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = RANDOM_PORT)
class JsonScoringControllerSpec extends ScoringBase[String, String, String] {

  lazy val printer = new Printer(includingDefaultValueFields = true, formattingLongAsNumber = true)
  lazy val parser = new Parser()

  override def createLoadModelRequest(modelName: String, uri:URI, createTmpFile: Boolean): HttpEntity[String] = {
    val loadModelRequest = LoadModelRequest(modelName = modelName,
      uri = getBundle(uri, createTmpFile),
      config = ModelConfig(memoryTimeout = 15.minutes, diskTimeout = 15.minutes))

    val jsonRequest = JsonMethods.compact(printer.toJson(
      pb.LoadModelRequest(modelName = loadModelRequest.modelName,
                          uri = loadModelRequest.uri.toString,
                          config = Some(TypeConverters.executorToPbModelConfig(loadModelRequest.config)),
                          force = loadModelRequest.force)))
    new HttpEntity[String](jsonRequest, JsonScoringControllerSpec.jsonHeaders)
  }

  override def extractModelResponse(response: ResponseEntity[_ <: Any]): Mleap.Model =
    Model.toJavaProto(parser.fromJsonString[Model](response.getBody.asInstanceOf[String]))

  override def createEmptyBodyRequest(): HttpEntity[Unit] = JsonScoringControllerSpec.httpEntityWithJsonHeaders

  override def extractBundleMetaResponse(response: ResponseEntity[_]): Mleap.BundleMeta =
    BundleMeta.toJavaProto(parser.fromJsonString[BundleMeta](response.getBody.asInstanceOf[String]))

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
