package ml.combust.mleap.springboot

import akka.actor.ActorSystem
import ml.combust.mleap.executor.MleapExecutor
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter

import scalapb.json4s.{Parser, Printer}

object StarterConfiguration {
  private var actorSystem: Option[ActorSystem] = None

  def setActorSystem(system: ActorSystem): Unit = {
    this.actorSystem = Option(system)
  }

  def getActorSystem: ActorSystem = this.actorSystem.getOrElse {
    ActorSystem("MleapSpringBoot")
  }

  def getMleapExecutor: MleapExecutor = MleapExecutor(getActorSystem)
}

@Configuration
@EnableConfigurationProperties
class StarterConfiguration {

  @Bean
  def actorSystem: ActorSystem = StarterConfiguration.getActorSystem

  @Bean
  def mleapExecutor(actorSystem: ActorSystem) = StarterConfiguration.getMleapExecutor

  @Bean
  def protobufHttpMessageConverter() = new ProtobufHttpMessageConverter

  @Bean
  def jsonPrinter() = new Printer(includingDefaultValueFields = true, formattingLongAsNumber = true)

  @Bean
  def jsonParser() = new Parser()
}