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
import ml.combust.mleap.pb.{BundleMeta, Model}
import org.springframework.http.converter.StringHttpMessageConverter

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = RANDOM_PORT)
class JsonScoringControllerSpec extends FunSpec with Matchers {

  @Autowired
  var restTemplate: TestRestTemplate = _
  new TestContextManager(this.getClass).prepareTestInstance(this)
  restTemplate.getRestTemplate.setMessageConverters(util.Arrays.asList(new ProtobufHttpMessageConverter(),
    new StringHttpMessageConverter()))

  def waitUntilModelLoaded(modelName: String, retries: Int): Boolean = {
    for (_ <- 1 to retries) {
      val response = restTemplate.exchange("/models/" + modelName, HttpMethod.GET, httpEntityWithJsonHeaders, classOf[String])
      if (response.getStatusCode == HttpStatus.OK) {
        return true
      } else {
        // pause a bit to allow the model to load
        Thread.sleep(500)
      }
    }

    false
  }

  describe("json scoring controller - load model endpoint") {
    it("loading model is successful") {
      val request = buildLoadModelJsonRequest("model1", demoUri, true)
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String](request, jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.OK)
      val model = parser.fromJsonString[Model](response.getBody)
      assert(model.name == "model1")
      assert(model.uri.toString.contains("demo"))
    }

    it("returns BAD_REQUEST error with empty request") {
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String]("", jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("returns BAD_REQUEST error with empty model name") {
      val request = buildLoadModelJsonRequest("", demoUri, true)
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String](request, jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("returns BAD_REQUEST error when loading model with the same name twice") { // because of not unique actor name
      // load model first
      val loadModelRequest1 = buildLoadModelJsonRequest("model2", demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, new HttpEntity[String](loadModelRequest1, jsonHeaders), classOf[String])

      // wait until it's been loaded
      if (!waitUntilModelLoaded("model2", 4)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      // try to load model with the same name again
      val loadModelRequest2 = buildLoadModelJsonRequest("model2", demoUri, true)
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String](loadModelRequest2, jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("doesn't return error when empty uri"){
      val request = buildLoadModelJsonRequest("model3", URI.create(""), false)
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String](request, jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.OK)
      val model = parser.fromJsonString[Model](response.getBody)
      assert(model.name == "model3")
    }

    it("doesn't return error when bundle doesn't exist at URI") {
      val request = buildLoadModelJsonRequest("model4", URI.create("file://dummy"), false)
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String](request, jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.OK)
      val model = parser.fromJsonString[Model](response.getBody)
      assert(model.name == "model4")
    }

    it("doesn't return error when no repository can handle request") {
      val request = buildLoadModelJsonRequest("model5", URI.create("dummy"), false)
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String](request, jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.OK)
      val model = parser.fromJsonString[Model](response.getBody)
      assert(model.name == "model5")
    }

    it("returns BAD_REQUEST error when request is not valid json") {
      val response = restTemplate.exchange("/models", HttpMethod.POST,
        new HttpEntity[String]("{invalid json}", jsonHeaders), classOf[String])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }
  }

  describe("json scoring controller - unload model endpoint") {
    it("unloads model successfully") {
      // load model first
      val loadModelRequest1 = buildLoadModelJsonRequest("model6", demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, new HttpEntity[String](loadModelRequest1, jsonHeaders), classOf[String])

      // wait until it's been loaded
      if (!waitUntilModelLoaded("model6", 4)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      val response = restTemplate.exchange("/models/model6", HttpMethod.DELETE, httpEntityWithJsonHeaders, classOf[String])
      assert(response.getStatusCode == HttpStatus.OK)
      val model = parser.fromJsonString[Model](response.getBody)
      assert(model.name == "model6")
    }

    it("returns NOT_FOUND error when no model loaded previously") {
      val response = restTemplate.exchange("/models/missing", HttpMethod.DELETE, httpEntityWithJsonHeaders, classOf[String])
      assert(response.getStatusCode == HttpStatus.NOT_FOUND)
    }

    it("returns NOT_FOUND error when unloading model multiple times") {
      // load model first
      val loadModelRequest1 = buildLoadModelJsonRequest("model7", demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, new HttpEntity[String](loadModelRequest1, jsonHeaders), classOf[String])

      // wait until it's been loaded
      if (!waitUntilModelLoaded("model7", 4)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      val response = restTemplate.exchange("/models/model7", HttpMethod.DELETE, httpEntityWithJsonHeaders, classOf[String])
      assert(response.getStatusCode == HttpStatus.OK)

      val secondResponse = restTemplate.exchange("/models/model7", HttpMethod.DELETE, httpEntityWithJsonHeaders, classOf[String])
      assert(secondResponse.getStatusCode == HttpStatus.NOT_FOUND)

      val thirdResponse = restTemplate.exchange("/models/model7", HttpMethod.DELETE, httpEntityWithJsonHeaders, classOf[String])
      assert(thirdResponse.getStatusCode == HttpStatus.NOT_FOUND)

      val fourthResponse = restTemplate.exchange("/models/model7", HttpMethod.DELETE, httpEntityWithJsonHeaders, classOf[String])
      assert(fourthResponse.getStatusCode == HttpStatus.NOT_FOUND)
    }
  }

  describe("json scoring controller - get model endpoint") {
    it("gets model successfully") {
      // load model first
      val loadModelRequest1 = buildLoadModelJsonRequest("model8", demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, new HttpEntity[String](loadModelRequest1, jsonHeaders), classOf[String])

      // wait until it's been loaded
      if (!waitUntilModelLoaded("model8", 4)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      val response = restTemplate.exchange("/models/model8", HttpMethod.GET, httpEntityWithJsonHeaders, classOf[String])
      assert(response.getStatusCode == HttpStatus.OK)
      val model = parser.fromJsonString[Model](response.getBody)
      assert(model.name == "model8")
    }

    it("returns NOT_FOUND error when no model loaded previously") {
      val response = restTemplate.exchange("/models/missing", HttpMethod.GET, httpEntityWithJsonHeaders, classOf[String])
      assert(response.getStatusCode == HttpStatus.NOT_FOUND)
    }
  }


  describe("json scoring controller - get model META endpoint") {
    it("gets model successfully") {
      // load model first
      val loadModelRequest1 = buildLoadModelJsonRequest("model9", demoUri, true)
      restTemplate.exchange("/models", HttpMethod.POST, new HttpEntity[String](loadModelRequest1, jsonHeaders), classOf[String])

      // wait until it's been loaded
      if (!waitUntilModelLoaded("model9", 4)) {
        fail("model hasn't been loaded successfully the first time, the test cannot succeed")
      }

      val response = restTemplate.exchange("/models/model9/meta", HttpMethod.GET, httpEntityWithJsonHeaders, classOf[String])
      assert(response.getStatusCode == HttpStatus.OK)
      val meta = parser.fromJsonString[BundleMeta](response.getBody)
      assert(meta.getBundle.name == "pipeline_7a70bdf8-bd53-11e7-bcd7-6c40089417e6")
    }

    it("returns NOT_FOUND error when no model loaded previously") {
      val response = restTemplate.exchange("/models/missing/meta", HttpMethod.GET, httpEntityWithJsonHeaders, classOf[String])
      assert(response.getStatusCode == HttpStatus.NOT_FOUND)
    }
  }
}
