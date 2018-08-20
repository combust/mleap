package ml.combust.mleap.springboot

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties

@SpringBootApplication
@EnableConfigurationProperties
class RunServer {
  def run(): Unit = {
    SpringApplication.run(classOf[RunServer])
  }
}
