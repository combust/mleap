package ml.combust.mleap.springboot

import java.net.URI
import java.util
import java.util.UUID

import ml.combust.mleap.pb.Mleap
import ml.combust.mleap.springboot.TestUtil.demoUri
import org.scalatest.{FunSpec, Matchers}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.{HttpEntity, HttpMethod, HttpStatus, ResponseEntity}
import org.springframework.http.converter.StringHttpMessageConverter
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter
import org.springframework.test.context.TestContextManager
import scala.reflect._

abstract class ScoringBase[T, U, V](implicit cu: ClassTag[U], cv: ClassTag[V]) extends FunSpec with Matchers {

  @Autowired
  var restTemplate: TestRestTemplate = _
  new TestContextManager(this.getClass).prepareTestInstance(this)
  restTemplate.getRestTemplate.setMessageConverters(util.Arrays.asList(new ProtobufHttpMessageConverter(),
    new StringHttpMessageConverter()))

  def createLoadModelRequest(modelName: String, uri:URI, createTmpFile: Boolean): HttpEntity[T]

  def extractModelResponse(response: ResponseEntity[_ <: Any]) : Mleap.Model

  def createEmptyBodyRequest(): HttpEntity[Unit]

  def extractBundleMetaResponse(response: ResponseEntity[_ <: Any]): Mleap.BundleMeta

  describe("scoring controller - load model endpoint") {
    it("loading model is successful") {
      val modelName = UUID.randomUUID().toString
      val request = createLoadModelRequest(modelName, demoUri, true)
      val response = restTemplate.exchange("/models", HttpMethod.POST, request, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.OK)
      val model = extractModelResponse(response)
      assert(model.getName == modelName)
      assert(model.getUri.contains("demo"))
    }

    it("returns BAD_REQUEST error with empty model name") {
      val request = createLoadModelRequest("", demoUri, true)
      val response = restTemplate.exchange("/models", HttpMethod.POST, request, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("returns BAD_REQUEST error when loading model with the same name twice") { // because of not unique actor name
      val modelName = UUID.randomUUID().toString
      // load model first
      val loadModelRequest1 = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest1, cu.runtimeClass)

      // wait until it's been loaded
      if (!waitUntilModelLoaded(modelName, 4)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      // try to load model with the same name again
      val loadModelRequest2 = createLoadModelRequest(modelName, demoUri, true)
      val response = restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest2, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("doesn't return error when empty uri"){
      val modelName = UUID.randomUUID().toString
      val request = createLoadModelRequest(modelName, URI.create(""), false)
      val response = restTemplate.exchange("/models", HttpMethod.POST, request, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.OK)
      val model = extractModelResponse(response)
      assert(model.getName == modelName)
    }

    it("doesn't return error when bundle doesn't exist at URI") {
      val modelName = UUID.randomUUID().toString
      val request = createLoadModelRequest(modelName, URI.create("file://dummy"), false)
      val response = restTemplate.exchange("/models", HttpMethod.POST, request, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.OK)
      val model = extractModelResponse(response)
      assert(model.getName == modelName)
    }

    it("doesn't return error when no repository can handle request") {
      val modelName = UUID.randomUUID().toString
      val request = createLoadModelRequest(modelName, URI.create("dummy"), false)
      val response = restTemplate.exchange("/models", HttpMethod.POST, request, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.OK)
      val model =extractModelResponse(response)
      assert(model.getName == modelName)
    }
  }

  describe("scoring controller - unload model endpoint") {
    it("unloads model successfully") {
      val modelName = UUID.randomUUID().toString
      // load model first
      val loadModelRequest1 = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest1, cu.runtimeClass)

      // wait until it's been loaded
      if (!waitUntilModelLoaded(modelName, 4)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      val response = restTemplate.exchange("/models/" + modelName, HttpMethod.DELETE, createEmptyBodyRequest(), cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.OK)
      val model = extractModelResponse(response)
      assert(model.getName == modelName)
    }

    it("returns NOT_FOUND error when no model loaded previously") {
      val response = restTemplate.exchange("/models/missing", HttpMethod.DELETE, createEmptyBodyRequest(), cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.NOT_FOUND)
    }

    it("returns NOT_FOUND error when unloading model multiple times") {
      val modelName = UUID.randomUUID().toString
      // load model first
      val loadModelRequest1 = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest1, cu.runtimeClass)

      // wait until it's been loaded
      if (!waitUntilModelLoaded(modelName, 4)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      val response = restTemplate.exchange("/models/" + modelName, HttpMethod.DELETE, createEmptyBodyRequest(), cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.OK)

      val secondResponse = restTemplate.exchange("/models/" + modelName, HttpMethod.DELETE, createEmptyBodyRequest(), cu.runtimeClass)
      assert(secondResponse.getStatusCode == HttpStatus.NOT_FOUND)

      val thirdResponse = restTemplate.exchange("/models/" + modelName, HttpMethod.DELETE, createEmptyBodyRequest(), cu.runtimeClass)
      assert(thirdResponse.getStatusCode == HttpStatus.NOT_FOUND)

      val fourthResponse = restTemplate.exchange("/models/" + modelName, HttpMethod.DELETE, createEmptyBodyRequest(), cu.runtimeClass)
      assert(fourthResponse.getStatusCode == HttpStatus.NOT_FOUND)
    }
  }

  describe("scoring controller - get model endpoint") {
    it("gets model successfully") {
      val modelName = UUID.randomUUID().toString
      // load model first
      val loadModelRequest1 = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest1, cu.runtimeClass)

      // wait until it's been loaded
      if (!waitUntilModelLoaded(modelName, 4)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      val response = restTemplate.exchange("/models/" + modelName, HttpMethod.GET, createEmptyBodyRequest(), cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.OK)
      val model = extractModelResponse(response)
      assert(model.getName == modelName)
    }

    it("returns NOT_FOUND error when no model loaded previously") {
      val response = restTemplate.exchange("/models/missing", HttpMethod.GET, createEmptyBodyRequest(), cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.NOT_FOUND)
    }
  }

  describe("scoring controller - get bundle META endpoint") {
    it("gets model successfully") {
      val modelName = UUID.randomUUID().toString
      // load model first
      val loadModelRequest1 = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest1, cu.runtimeClass)

      // wait until it's been loaded
      if (!waitUntilModelLoaded(modelName, 4)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      val response = restTemplate.exchange("/models/" + modelName + "/meta", HttpMethod.GET, createEmptyBodyRequest(), cv.runtimeClass)
      assert(response.getStatusCode == HttpStatus.OK)
      val meta = extractBundleMetaResponse(response)
      assert(meta.getBundle.getName == "pipeline_7a70bdf8-bd53-11e7-bcd7-6c40089417e6")
    }

    it("returns NOT_FOUND error when no model loaded previously") {
      val response = restTemplate.exchange("/models/missing/meta", HttpMethod.GET, createEmptyBodyRequest(), cv.runtimeClass)
      assert(response.getStatusCode == HttpStatus.NOT_FOUND)
    }
  }

  def waitUntilModelLoaded(modelName: String, retries: Int): Boolean = {
    for (_ <- 1 to retries) {
      val response = restTemplate.exchange("/models/" + modelName, HttpMethod.GET, JsonScoringControllerSpec.httpEntityWithJsonHeaders, classOf[String])
      if (response.getStatusCode == HttpStatus.OK) {
        return true
      } else {
        // pause a bit to allow the model to load
        Thread.sleep(500)
      }
    }

    false
  }
}
