package ml.combust.mleap.springboot

import akka.actor.ActorSystem
import ml.combust.mleap.executor.MleapExecutor
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
class StarterConfiguration {

  @Bean
  def system() = {
    ActorSystem("MleapSpringBootScoring")
  }

  @Bean
  def mleapExecutor(system: ActorSystem) = {
    MleapExecutor(system)
  }

}