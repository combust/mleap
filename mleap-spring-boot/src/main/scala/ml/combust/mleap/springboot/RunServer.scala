package ml.combust.mleap.springboot

import akka.actor.ActorSystem
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties

@SpringBootApplication
@EnableConfigurationProperties
class RunServerApplication

class RunServer(actorSystem: Option[ActorSystem] = None) {
  def run(): Unit = {
    for (as <- actorSystem) { StarterConfiguration.setActorSystem(as) }
    SpringApplication.run(classOf[RunServerApplication])
  }
}
