package ml.combust.mleap.springboot

import akka.actor.ActorSystem
import ml.combust.mleap.executor.MleapExecutor
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter

@Configuration
class StarterConfiguration {

  @Bean
  def actorSystem() = {
    ActorSystem("MleapSpringBootScoring")
  }

  @Bean
  def mleapExecutor(actorSystem: ActorSystem) = {
    MleapExecutor(actorSystem)
  }

  @Bean
  def protobufHttpMessageConverter() = {
    new ProtobufHttpMessageConverter
  }

}