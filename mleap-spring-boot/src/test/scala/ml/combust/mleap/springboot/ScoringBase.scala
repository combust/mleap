package ml.combust.mleap.springboot

import java.net.URI
import java.util
import java.util.UUID

import ml.combust.mleap.pb.SelectMode.{SELECT_MODE_RELAXED, SELECT_MODE_STRICT}
import ml.combust.mleap.pb.{Mleap, TransformOptions}
import ml.combust.mleap.springboot.TestUtil._
import org.scalatest.{FunSpec, Matchers}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.{HttpEntity, HttpMethod, HttpStatus, ResponseEntity}
import org.springframework.http.converter.{ByteArrayHttpMessageConverter, StringHttpMessageConverter}
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter
import org.springframework.test.context.TestContextManager
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.combust.mleap.runtime.serialization.FrameReader

import scala.reflect._

abstract class ScoringBase[T, U, V, X, Y](implicit cu: ClassTag[U], cv: ClassTag[V], cy: ClassTag[Y]) extends FunSpec with Matchers {

  @Autowired
  var restTemplate: TestRestTemplate = _
  new TestContextManager(this.getClass).prepareTestInstance(this)
  restTemplate.getRestTemplate.setMessageConverters(util.Arrays.asList(new ProtobufHttpMessageConverter(),
    new StringHttpMessageConverter(), new ByteArrayHttpMessageConverter()))

  def createLoadModelRequest(modelName: String, uri:URI, createTmpFile: Boolean): HttpEntity[T]

  def extractModelResponse(response: ResponseEntity[_ <: Any]) : Mleap.Model

  def createEmptyBodyRequest(): HttpEntity[Unit]

  def extractBundleMetaResponse(response: ResponseEntity[_ <: Any]): Mleap.BundleMeta

  def createTransformFrameRequest(modelName: String, frame: DefaultLeapFrame, options: Option[TransformOptions]) : HttpEntity[X]

  def createTransformFrameRequest(frame: DefaultLeapFrame): HttpEntity[Array[Byte]]

  def createInvalidTransformFrameRequest(modelName: String, bytes: Array[Byte]) : HttpEntity[X]

  def extractTransformResponse(response: ResponseEntity[_ <: Any]): Mleap.TransformFrameResponse

  def leapFrameFormat(): String

  describe("scoring controller - load model endpoint") {
    it("loading model is successful") {
      val modelName = UUID.randomUUID().toString
      val request = createLoadModelRequest(modelName, demoUri, true)
      val response = restTemplate.exchange("/models", HttpMethod.POST, request, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.ACCEPTED)
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

      waitUntilModelLoaded(modelName, 10)

      // try to load model with the same name again
      val loadModelRequest2 = createLoadModelRequest(modelName, demoUri, true)
      val response = restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest2, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("doesn't return error when empty uri"){
      val modelName = UUID.randomUUID().toString
      val request = createLoadModelRequest(modelName, URI.create(""), false)
      val response = restTemplate.exchange("/models", HttpMethod.POST, request, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.ACCEPTED)
      val model = extractModelResponse(response)
      assert(model.getName == modelName)
    }

    it("doesn't return error when bundle doesn't exist at URI") {
      val modelName = UUID.randomUUID().toString
      val request = createLoadModelRequest(modelName, URI.create("file://dummy"), false)
      val response = restTemplate.exchange("/models", HttpMethod.POST, request, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.ACCEPTED)
      val model = extractModelResponse(response)
      assert(model.getName == modelName)
    }

    it("doesn't return error when no repository can handle request") {
      val modelName = UUID.randomUUID().toString
      val request = createLoadModelRequest(modelName, URI.create("dummy"), false)
      val response = restTemplate.exchange("/models", HttpMethod.POST, request, cu.runtimeClass)
      assert(response.getStatusCode == HttpStatus.ACCEPTED)
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

      waitUntilModelLoaded(modelName, 10)

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

      waitUntilModelLoaded(modelName, 10)

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

      waitUntilModelLoaded(modelName, 10)

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
    it("gets bundle meta successfully") {
      val modelName = UUID.randomUUID().toString
      // load model first
      val loadModelRequest1 = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest1, cu.runtimeClass)

      waitUntilModelLoaded(modelName, 10)

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

  describe("scoring controller - transform endpoint") {
    it("transforms request successfully") {
      val modelName = UUID.randomUUID().toString
      val loadModelRequest = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest, cu.runtimeClass)

      waitUntilModelLoaded(modelName, 10)

      val request = createTransformFrameRequest(modelName, validFrame, None)
      val response = restTemplate.exchange("/models/transform", HttpMethod.POST, request, cy.runtimeClass)
      val transformResponse = extractTransformResponse(response)
      assert(transformResponse.getStatus == Mleap.TransformStatus.STATUS_OK)

      val data = FrameReader(leapFrameFormat()).fromBytes(transformResponse.getFrame.toByteArray).get.dataset.toArray
      assert(data(0).getDouble(5) == -67.78953193834998)
    }

    it("transforms request successfully with valid strict selecting") {
      val modelName = UUID.randomUUID().toString
      val loadModelRequest = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest, cu.runtimeClass)

      waitUntilModelLoaded(modelName, 10)

      val request = createTransformFrameRequest(modelName, validFrame,
        Some(TransformOptions(select = Seq("demo:prediction"), selectMode = SELECT_MODE_STRICT)))
      val response = restTemplate.exchange("/models/transform", HttpMethod.POST, request, cy.runtimeClass)
      val transformResponse = extractTransformResponse(response)
      assert(transformResponse.getStatus == Mleap.TransformStatus.STATUS_OK)

      val data = FrameReader(leapFrameFormat()).fromBytes(transformResponse.getFrame.toByteArray).get.dataset.toArray
      assert(data(0).getDouble(0) == -67.78953193834998)
    }

    it("transforms request successfully with some valid and some invalid relaxed selecting") {
      val modelName = UUID.randomUUID().toString
      val loadModelRequest = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest, cu.runtimeClass)

      if (!waitUntilModelLoaded(modelName, 10)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      val request = createTransformFrameRequest(modelName, validFrame,
        Some(TransformOptions(select = Seq("demo:prediction", "dummy"), selectMode = SELECT_MODE_RELAXED)))
      val response = restTemplate.exchange("/models/transform", HttpMethod.POST, request, cy.runtimeClass)
      val transformResponse = extractTransformResponse(response)
      assert(transformResponse.getStatus == Mleap.TransformStatus.STATUS_OK)

      val data = FrameReader(leapFrameFormat()).fromBytes(transformResponse.getFrame.toByteArray).get.dataset.toArray
      assert(data(0).getDouble(0) == -67.78953193834998)
    }

    it("returns OK response with error wrapped in TransformFrameResponse with some valid and some invalid strict selecting") {
      val modelName = UUID.randomUUID().toString
      val loadModelRequest = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest, cu.runtimeClass)

      waitUntilModelLoaded(modelName, 10)

      val request = createTransformFrameRequest(modelName, validFrame,
        Some(TransformOptions(select = Seq("demo:prediction", "dummy"), selectMode = SELECT_MODE_STRICT)))
      val response = restTemplate.exchange("/models/transform", HttpMethod.POST, request, cy.runtimeClass)
      assertTransformError(response)

    }

    it("returns OK response with error wrapped in TransformFrameResponse when no model loaded previously") {
      val modelName = UUID.randomUUID().toString
      val request = createTransformFrameRequest(modelName, validFrame, None)
      val response = restTemplate.exchange("/models/transform", HttpMethod.POST, request, cy.runtimeClass)
      assertTransformError(response)
    }

    it("returns OK response with error wrapped in TransformFrameResponse when transform fails") {
      val modelName = UUID.randomUUID().toString
      val request = createTransformFrameRequest(modelName, incompleteFrame, None)

      val loadModelRequest = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest, cu.runtimeClass)

      waitUntilModelLoaded(modelName, 10)

      val response = restTemplate.exchange("/models/transform", HttpMethod.POST, request, cy.runtimeClass)
      assertTransformError(response)
    }

    it("returns OK response with error wrapped in TransformFrameResponse when leapframe parsing fails") {
      val modelName = UUID.randomUUID().toString
      val request = createInvalidTransformFrameRequest(modelName, failingBytes)

      val loadModelRequest = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest, cu.runtimeClass)

      waitUntilModelLoaded(modelName, 10)

      val response = restTemplate.exchange("/models/transform", HttpMethod.POST, request, cy.runtimeClass)
      assertTransformError(response)
    }

    it("transforms leap frame directly successfully") {
      val modelName = UUID.randomUUID().toString
      val loadModelRequest = createLoadModelRequest(modelName, demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, loadModelRequest, cu.runtimeClass)

      waitUntilModelLoaded(modelName, 10)

      val request = createTransformFrameRequest(validFrame)
      val response = restTemplate.exchange("/models/" + modelName + "/transform", HttpMethod.POST, request, classOf[Array[Byte]])
      assert(response.getStatusCode == HttpStatus.OK)

      val data = FrameReader(leapFrameFormat()).fromBytes(response.getBody).get.dataset.toArray
      assert(data(0).getDouble(5) == -67.78953193834998)
    }
  }

  private def assertTransformError(response: ResponseEntity[_ <: Any]) = {
    assert(response.getStatusCode == HttpStatus.OK)
    val transformResponse = extractTransformResponse(response)
    assert(transformResponse.getStatus == Mleap.TransformStatus.STATUS_ERROR)
    assert(!transformResponse.getError.isEmpty)
    assert(!transformResponse.getBacktrace.isEmpty)
  }

  def waitUntilModelLoaded(modelName: String, retries: Int): Boolean = {
    for (_ <- 1 to retries) {
      val response = restTemplate.exchange("/models/" + modelName, HttpMethod.GET, JsonScoringSpec.httpEntityWithJsonHeaders, classOf[String])
      if (response.getStatusCode == HttpStatus.OK) {
        // pause a bit to ensure model has finished loaded
        Thread.sleep(500)
        return true
      } else {
        // pause a bit to allow the model to load
        Thread.sleep(500)
      }
    }

    fail("model hasn't been loaded successfully the first time, the test cannot succeed")
  }
}
