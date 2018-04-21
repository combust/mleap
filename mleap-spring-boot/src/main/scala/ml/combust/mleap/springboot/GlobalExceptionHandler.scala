package ml.combust.mleap.springboot

import javax.servlet.http.HttpServletRequest

import akka.pattern.AskTimeoutException
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.{ControllerAdvice, ExceptionHandler}

@ControllerAdvice
@Component
class GlobalExceptionHandler {

  @ExceptionHandler(Array(classOf[IllegalArgumentException]))
  def handleClientErrors(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =  {
    ResponseEntity.badRequest().build()
  }

  @ExceptionHandler(Array(classOf[AskTimeoutException]))
  def handleServerErrors(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =  {
    ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build()
  }
}
