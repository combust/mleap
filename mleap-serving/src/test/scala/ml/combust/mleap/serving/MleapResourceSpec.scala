package ml.combust.mleap.serving

import akka.http.javadsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import ml.combust.mleap.serving.domain.v1.LoadModelRequest
import ml.combust.mleap.serving.marshalling.{ApiMarshalling, LeapFrameMarshalling}
import org.scalatest.{FunSpec, Matchers}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ml.combust.mleap.runtime.DefaultLeapFrame

class MleapResourceSpec extends FunSpec with Matchers with ScalatestRouteTest with LeapFrameMarshalling with ApiMarshalling {

  val resource = new MleapResource(new MleapService())

  describe("/model path") {
    it("returns OK for PUT requests") {
      val bundlePath = TestUtil.serializeModelInJsonFormatToZipFile
      Put("/model", LoadModelRequest(Some(bundlePath))) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    it("returns INTERNAL_SERVER_ERROR for PUT requests with no model provided") {
      Put("/model", LoadModelRequest(None)) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.INTERNAL_SERVER_ERROR
      }
    }

    it("returns INTERNAL_SERVER_ERROR for PUT requests with unknown model provided") {
      Put("/model", LoadModelRequest(Some("/tmp/unknown.json.zip"))) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.INTERNAL_SERVER_ERROR
      }
    }

    it("returns OK for DELETE requests") {
      Delete("/model") ~> resource.routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    it("returns a METHOD_NOT_ALLOWED error for GET requests") {
      Get("/model") ~> Route.seal(resource.routes) ~> check {
        status shouldEqual StatusCodes.METHOD_NOT_ALLOWED
        responseAs[String] shouldEqual "HTTP method not allowed, supported methods: PUT, DELETE"
      }
    }
  }

  describe("/transform path") {
    it("returns INTERNAL_SERVER_ERROR for POST requests if no model has been loaded yet") {
      Post("/transform", TestUtil.getLeapFrame) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.INTERNAL_SERVER_ERROR
      }
    }

    it("transforms the leap frame for POST requests if model has been previously loaded") {
      val bundlePath = TestUtil.serializeModelInJsonFormatToZipFile
      Put("/model", LoadModelRequest(Some(bundlePath))) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.OK
      }
      Post("/transform", TestUtil.getLeapFrame) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.OK
        val outputLeapFrame = responseAs[DefaultLeapFrame]
        val data = outputLeapFrame.dataset.toArray
        assert(data(0).getDouble(4) == 24.0)
        assert(data(1).getDouble(4) == 19.0)
        assert(data(2).getDouble(4) == 23.0)
      }
    }

    it("returns a METHOD_NOT_ALLOWED error for GET requests") {
      Get("/transform", TestUtil.getLeapFrame) ~> Route.seal(resource.routes) ~> check {
        status shouldEqual StatusCodes.METHOD_NOT_ALLOWED
        responseAs[String] shouldEqual "HTTP method not allowed, supported methods: POST"
      }
    }

    it("returns a METHOD_NOT_ALLOWED error for PUT requests") {
      Put("/transform", TestUtil.getLeapFrame) ~> Route.seal(resource.routes) ~> check {
        status shouldEqual StatusCodes.METHOD_NOT_ALLOWED
        responseAs[String] shouldEqual "HTTP method not allowed, supported methods: POST"
      }
    }
  }
}