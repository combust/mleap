package ml.combust.mleap.springboot

import javax.servlet.http.HttpServletRequest

import akka.pattern.AskTimeoutException
import ml.combust.mleap.executor.repository.BundleException
import org.slf4j.LoggerFactory
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.{ControllerAdvice, ExceptionHandler}

@ControllerAdvice
@Component
class GlobalExceptionHandler {

  @ExceptionHandler(Array(classOf[BundleException]))
  def handleClientErrors(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =  {
    GlobalExceptionHandler.logger.warn("Returned client error due to ", ex)
    ResponseEntity.badRequest().build()
  }

  @ExceptionHandler(Array(classOf[AskTimeoutException], classOf[RuntimeException]))
  def handleServerErrors(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =  {
    GlobalExceptionHandler.logger.error("Returned server error due to ", ex)
    ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build()
  }
}

object GlobalExceptionHandler {
  val logger = LoggerFactory.getLogger(classOf[GlobalExceptionHandler])
}