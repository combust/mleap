package ml.combust.mleap.serving

import akka.http.javadsl.model.StatusCodes
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.server.Route
import ml.combust.mleap.serving.domain.v1.LoadModelRequest
import ml.combust.mleap.serving.marshalling.{ApiMarshalling, JsonSupport, LeapFrameMarshalling}
import org.scalatest.{FunSpec, Matchers}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ml.combust.mleap.runtime.DefaultLeapFrame
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import ml.combust.mleap.core.types.{DoubleType, StructType, TensorType}
import ml.combust.mleap.runtime.types.TensorType

class MleapResourceSpec extends FunSpec with Matchers with ScalatestRouteTest with LeapFrameMarshalling with ApiMarshalling {

  implicit val mleapLoadModelRequestEntityMarshaller: ToEntityMarshaller[LoadModelRequest] = JsonSupport.mleapLoadModelRequestFormat

  describe("/model path") {
    it("returns OK for PUT requests to load model") {
      val resource = new MleapResource(new MleapService())
      val bundlePath = TestUtil.serializeModelInJsonFormatToZipFile
      Put("/model", LoadModelRequest(Some(bundlePath))) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    it("returns INTERNAL_SERVER_ERROR for PUT requests with no model provided") {
      val resource = new MleapResource(new MleapService())
      Put("/model", LoadModelRequest(None)) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.INTERNAL_SERVER_ERROR
      }
    }

    it("returns INTERNAL_SERVER_ERROR for PUT requests with unknown model provided") {
      val resource = new MleapResource(new MleapService())
      Put("/model", LoadModelRequest(Some("/tmp/unknown.json.zip"))) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.INTERNAL_SERVER_ERROR
      }
    }

    it("returns OK for DELETE requests to unload model") {
      val resource = new MleapResource(new MleapService())
      Delete("/model") ~> resource.routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    it("retrieves the model schema for GET requests if model has been previously loaded") {
      val resource = new MleapResource(new MleapService())
      val bundlePath = TestUtil.serializeModelInJsonFormatToZipFile
      Put("/model", LoadModelRequest(Some(bundlePath))) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.OK
      }

      Get("/model") ~> resource.routes ~> check {
        status shouldEqual StatusCodes.OK
        val schema = responseAs[StructType]
        assert(schema.fields.size == 5)
        assert(schema.getField("first_double").get.dataType == DoubleType())
        assert(schema.getField("second_double").get.dataType == DoubleType())
        assert(schema.getField("third_double").get.dataType == DoubleType())
        assert(schema.getField("features").get.dataType == TensorType(DoubleType()))
        assert(schema.getField("prediction").get.dataType == DoubleType())
      }
    }

    it("returns a METHOD_NOT_ALLOWED error for POST requests") {
      val resource = new MleapResource(new MleapService())
      Post("/model") ~> Route.seal(resource.routes) ~> check {
        status shouldEqual StatusCodes.METHOD_NOT_ALLOWED
        responseAs[String] shouldEqual "HTTP method not allowed, supported methods: PUT, DELETE, GET"
      }
    }
  }

  describe("/transform path") {
    it("returns INTERNAL_SERVER_ERROR for POST requests if no model has been loaded yet") {
      val resource = new MleapResource(new MleapService())
      Post("/transform", TestUtil.getLeapFrame) ~> resource.routes ~> check {
        status shouldEqual StatusCodes.INTERNAL_SERVER_ERROR
      }
    }

    it("transforms the leap frame for POST requests if model has been previously loaded") {
      val resource = new MleapResource(new MleapService())
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
      val resource = new MleapResource(new MleapService())
      Get("/transform", TestUtil.getLeapFrame) ~> Route.seal(resource.routes) ~> check {
        status shouldEqual StatusCodes.METHOD_NOT_ALLOWED
        responseAs[String] shouldEqual "HTTP method not allowed, supported methods: POST"
      }
    }

    it("returns a METHOD_NOT_ALLOWED error for PUT requests") {
      val resource = new MleapResource(new MleapService())
      Put("/transform", TestUtil.getLeapFrame) ~> Route.seal(resource.routes) ~> check {
        status shouldEqual StatusCodes.METHOD_NOT_ALLOWED
        responseAs[String] shouldEqual "HTTP method not allowed, supported methods: POST"
      }
    }
  }
}