package ml.combust.mleap.springboot

import java.util

import ml.combust.mleap.pb.Mleap
import org.junit.runner.RunWith
import org.scalatest.{FunSpec, Matchers}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter
import org.springframework.test.context.TestContextManager
import org.springframework.http.HttpEntity
import org.springframework.http.HttpMethod
import TestUtil._
import ml.combust.mleap.pb.Mleap.TransformStatus
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameReader}

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = RANDOM_PORT)
class ScoringControllerSpec extends FunSpec with Matchers {

  @Autowired
  var testRestTemplate: TestRestTemplate = _
  new TestContextManager(this.getClass).prepareTestInstance(this)
  testRestTemplate.getRestTemplate.setMessageConverters(util.Arrays.asList(new ProtobufHttpMessageConverter()))

  describe("scoring controller") {
    it("retrieves bundle meta") {
      val url = s"/bundle-meta?uri=$demoUri"
      val response = testRestTemplate.exchange(url, HttpMethod.GET,
        new HttpEntity[String](protoHeaders), classOf[Mleap.BundleMeta])
      assert(response.getBody.getBundle.getName == "pipeline_7a70bdf8-bd53-11e7-bcd7-6c40089417e6")
    }

    it("transforms a leap frame") {
      val response = testRestTemplate.exchange("/transform/frame", HttpMethod.POST,
        new HttpEntity[Mleap.TransformFrameRequest](transformLeapFrameRequest, protoHeaders),
        classOf[Mleap.TransformFrameResponse])
      assert(response.getBody.getStatus == TransformStatus.STATUS_OK)

      val data = FrameReader(BuiltinFormats.binary).fromBytes(response.getBody.getFrame.toByteArray).get.dataset.toArray
      assert(data(0).getDouble(5) == -67.78953193834998)
    }
  }
}
