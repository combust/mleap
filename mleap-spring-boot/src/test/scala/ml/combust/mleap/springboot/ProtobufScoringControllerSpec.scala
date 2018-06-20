package ml.combust.mleap.springboot

import java.net.URI

import ml.combust.mleap.executor
import ml.combust.mleap.executor.ModelConfig
import ml.combust.mleap.pb
import ml.combust.mleap.pb.Mleap
import ml.combust.mleap.springboot.TestUtil._
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.http.{HttpEntity, HttpHeaders, ResponseEntity}
import org.springframework.test.context.junit4.SpringRunner

import scala.concurrent.duration._

@RunWith(classOf[SpringRunner])
@SpringBootTest(webEnvironment = RANDOM_PORT)
class ProtobufScoringControllerSpec extends ScoringBase[Mleap.LoadModelRequest, Mleap.Model, Mleap.BundleMeta] {

  override def createLoadModelRequest(modelName: String, uri: URI, createTmpFile: Boolean): HttpEntity[Mleap.LoadModelRequest] = {
    val loadModelRequest = executor.LoadModelRequest(modelName = modelName,
      uri = getBundle(uri, createTmpFile),
      config = ModelConfig(memoryTimeout = 15.minutes, diskTimeout = 15.minutes))

    val protoRequest = pb.LoadModelRequest.toJavaProto(pb.LoadModelRequest(modelName = loadModelRequest.modelName,
      uri = loadModelRequest.uri.toString,
      config = Some(TypeConverters.executorToPbModelConfig(loadModelRequest.config)),
      force = loadModelRequest.force))

    new HttpEntity[Mleap.LoadModelRequest](protoRequest, ProtobufScoringControllerSpec.protoHeaders)
  }

  override def extractModelResponse(response: ResponseEntity[_ <: Any]): Mleap.Model = response.getBody.asInstanceOf[Mleap.Model]

  override def createEmptyBodyRequest(): HttpEntity[Unit] = ProtobufScoringControllerSpec.httpEntityWithProtoHeaders

  override def extractBundleMetaResponse(response: ResponseEntity[_]): Mleap.BundleMeta = response.getBody.asInstanceOf[Mleap.BundleMeta]
}

object ProtobufScoringControllerSpec {
  lazy val httpEntityWithProtoHeaders = new HttpEntity[Unit](protoHeaders)

  lazy val protoHeaders = {
    val headers = new HttpHeaders
    headers.add("Content-Type", "application/x-protobuf")
    headers.add("timeout", "2000")
    headers
  }
}