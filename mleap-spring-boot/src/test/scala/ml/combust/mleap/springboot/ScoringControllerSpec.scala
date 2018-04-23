package ml.combust.mleap.springboot

import java.util

import ml.combust.mleap.pb.{Mleap, TransformFrameRequest}
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
import com.google.protobuf.ByteString
import ml.combust.mleap.pb.Mleap.TransformStatus
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameReader}

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = RANDOM_PORT)
class ScoringControllerSpec extends FunSpec with Matchers {

  @Autowired
  var restTemplate: TestRestTemplate = _
  new TestContextManager(this.getClass).prepareTestInstance(this)
  restTemplate.getRestTemplate.setMessageConverters(util.Arrays.asList(new ProtobufHttpMessageConverter()))

  describe("scoring controller") {

    //test case needs to be first or else it may not timeout
    it("returns INTERNAL_SERVER_ERROR when timeout") {
      val url = s"/bundle-meta?timeout=1&uri=$demoUri"
      val response = restTemplate.exchange(url, HttpMethod.GET, httpEntityWithProtoHeaders, classOf[Mleap.BundleMeta])
      assert(response.getStatusCode == HttpStatus.INTERNAL_SERVER_ERROR)
    }

    it("retrieves bundle meta") {
      val url = s"/bundle-meta?uri=$demoUri&timeout=2000"
      val response = restTemplate.exchange(url, HttpMethod.GET, httpEntityWithProtoHeaders, classOf[Mleap.BundleMeta])
      assert(response.getBody.getBundle.getName == "pipeline_7a70bdf8-bd53-11e7-bcd7-6c40089417e6")
    }

    it("returns BAD_REQUEST if bundle doesn't exist at given URI") {
      val url = s"/bundle-meta?uri=does_not_exist&timeout=2000"
      val response = restTemplate.exchange(url, HttpMethod.GET, httpEntityWithProtoHeaders, classOf[Mleap.BundleMeta])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("always returns BAD_REQUEST if bundle doesn't exist at given URI for subsequent requests") {
      val url = s"/bundle-meta?uri=does_not_exist&timeout=2000"
      val response = restTemplate.exchange(url, HttpMethod.GET, httpEntityWithProtoHeaders, classOf[Mleap.BundleMeta])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)

      val secondResponse = restTemplate.exchange(url, HttpMethod.GET, httpEntityWithProtoHeaders, classOf[Mleap.BundleMeta])
      assert(secondResponse.getStatusCode == HttpStatus.BAD_REQUEST)

      val thirdResponse = restTemplate.exchange(url, HttpMethod.GET, httpEntityWithProtoHeaders, classOf[Mleap.BundleMeta])
      assert(thirdResponse.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("can make a successful request to get bundle meta after a failed one") {
      // failed request
      val badUrl = s"/bundle-meta?uri=does_not_exist"
      restTemplate.exchange(badUrl, HttpMethod.GET, httpEntityWithProtoHeaders, classOf[Mleap.BundleMeta])
      // successful request
      val url = s"/bundle-meta?timeout=2000&uri=$demoUri"
      val response = restTemplate.exchange(url, HttpMethod.GET, httpEntityWithProtoHeaders, classOf[Mleap.BundleMeta])
      assert(response.getBody.getBundle.getName == "pipeline_7a70bdf8-bd53-11e7-bcd7-6c40089417e6")
    }

    it("returns BAD_REQUEST with empty URI") {
      val url = s"/bundle-meta?timeout=2000&uri="
      val response = restTemplate.exchange(url, HttpMethod.GET, httpEntityWithProtoHeaders, classOf[Mleap.BundleMeta])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }

    it("transforms a leap frame") {
      val request = TransformFrameRequest.toJavaProto(TransformFrameRequest(
        uri = demoUri,
        format = BuiltinFormats.binary,
        timeout = 2000L,
        frame = ByteString.copyFrom(leapFrame)))
      val response = restTemplate.exchange("/transform/frame", HttpMethod.POST,
        new HttpEntity[Mleap.TransformFrameRequest](request, protoHeaders), classOf[Mleap.TransformFrameResponse])
      assert(response.getBody.getStatus == TransformStatus.STATUS_OK)

      val data = FrameReader(BuiltinFormats.binary).fromBytes(response.getBody.getFrame.toByteArray).get.dataset.toArray
      assert(data(0).getDouble(5) == -67.78953193834998)
    }


    it("fails to transform an incomplete frame") {
      val request = TransformFrameRequest.toJavaProto(TransformFrameRequest(
        uri = demoUri,
        format = BuiltinFormats.binary,
        timeout = 2000L,
        frame = ByteString.copyFrom(incompleteLeapFrame)))
      val response = restTemplate.exchange("/transform/frame", HttpMethod.POST,
        new HttpEntity[Mleap.TransformFrameRequest](request, protoHeaders), classOf[Mleap.TransformFrameResponse])
      assert(response.getStatusCode == HttpStatus.INTERNAL_SERVER_ERROR)
    }

    it("fails transforming a frame when a non-existent bundle URI is given") {
      val request = TransformFrameRequest.toJavaProto(TransformFrameRequest(
        uri = "does-not-exist",
        format = BuiltinFormats.binary,
        timeout = 2000L,
        frame = ByteString.copyFrom(leapFrame)))
      val response = restTemplate.exchange("/transform/frame", HttpMethod.POST,
        new HttpEntity[Mleap.TransformFrameRequest](request, protoHeaders), classOf[Mleap.TransformFrameResponse])
      assert(response.getStatusCode == HttpStatus.BAD_REQUEST)
    }
  }
}
