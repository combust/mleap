package ml.combust.mleap.springboot

import TypeConverters._
import javax.annotation.PostConstruct
import org.slf4j.LoggerFactory
import ml.combust.mleap.pb
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Component

import scala.jdk.CollectionConverters._
import java.nio.file.{Files, Path, Paths}

import ml.combust.mleap.executor.MleapExecutor
import scalapb.json4s.Parser

@Component
class ModelLoader(@Autowired val mleapExecutor: MleapExecutor,
                  @Autowired val jsonParser: Parser) {

  @Value("${mleap.model.config:#{null}}")
  private val modelConfigPath: String = null

  private val logger = LoggerFactory.getLogger(classOf[ModelLoader])
  private val timeout = 60000

  @PostConstruct
  def loadModel(): Unit = {
    if (modelConfigPath == null) {
      logger.info("Skipping loading model on startup")
      return
    }

    val configPath = Paths.get(modelConfigPath)

    if (!Files.exists(configPath)) {
      logger.warn(s"Model path does not exist: $modelConfigPath")
      return
    }

    val configFiles: List[Path] = if (Files.isDirectory(configPath)) {
      Files.list(configPath).iterator().asScala.toList
    } else {
      List(configPath)
    }

    for (configFile <- configFiles) {
      logger.info(s"Loading model from ${configFile.toString}")

      val request = new String(Files.readAllBytes(configFile))

      mleapExecutor.loadModel(jsonParser.fromJsonString[pb.LoadModelRequest](request))(timeout)
    }
  }
}
