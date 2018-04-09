package ml.combust.mleap.spring

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties

@SpringBootApplication
@EnableConfigurationProperties
class Starter
object Starter extends App {
  SpringApplication.run(classOf[Starter])
}