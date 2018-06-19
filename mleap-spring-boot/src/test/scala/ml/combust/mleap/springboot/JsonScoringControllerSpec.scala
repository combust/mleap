package ml.combust.mleap.springboot

import java.net.URI
import java.util

import org.junit.runner.RunWith
import org.scalatest.{FunSpec, Matchers}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter
import org.springframework.test.context.TestContextManager
import org.springframework.http.{HttpEntity, HttpMethod, HttpStatus}
import TestUtil._
import org.springframework.http.converter.StringHttpMessageConverter

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = RANDOM_PORT)
class JsonScoringControllerSpec extends FunSpec with Matchers {

  @Autowired
  var restTemplate: TestRestTemplate = _
  new TestContextManager(this.getClass).prepareTestInstance(this)
  restTemplate.getRestTemplate.setMessageConverters(util.Arrays.asList(new ProtobufHttpMessageConverter(),
    new StringHttpMessageConverter()))

  describe("scoring controller - load model endpoint") {
    it("loading model is successful") {
      val request = buildLoadModelJsonRequest("model1", demoUri)
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String](request, jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.OK)
      val model = extractModelResponse(response.getBody)
      assert(model.name == "model1")
      assert(model.uri.toString.contains("demo"))
    }

    it("returns BAD_REQUEST error with empty request") {
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String]("", jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("returns BAD_REQUEST error with empty model name") {
      val request = buildLoadModelJsonRequest("", demoUri)
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String](request, jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("returns BAD_REQUEST error when loading the same model twice") {
      // because of not unique actor name
    }

    it("doesn't return error when empty uri"){
      val uri = URI.create("")
      val request = buildLoadModelJsonRequest("model3", uri)
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String](request, jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.OK)
      val model = extractModelResponse(response.getBody)
      assert(model.name == "model3")
      assert(model.uri.toString.contains("demo"))
    }

    it("doesn't return error when bundle doesn't exist at URI") {

    }
  }
}
