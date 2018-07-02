package ml.combust.mleap.springboot

import javax.servlet.http.HttpServletRequest

import akka.actor.InvalidActorNameException
import com.fasterxml.jackson.core.JsonProcessingException
import ml.combust.mleap.executor.error.{NotFoundException, TimeoutException}
import org.slf4j.LoggerFactory
import org.springframework.beans.{ConversionNotSupportedException, TypeMismatchException}
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.http.converter.{HttpMessageNotReadableException, HttpMessageNotWritableException}
import org.springframework.stereotype.Component
import org.springframework.validation.BindException
import org.springframework.web.bind.{MethodArgumentNotValidException, MissingPathVariableException, MissingServletRequestParameterException, ServletRequestBindingException}
import org.springframework.web.{HttpMediaTypeNotAcceptableException, HttpMediaTypeNotSupportedException, HttpRequestMethodNotSupportedException}
import org.springframework.web.bind.annotation.{ControllerAdvice, ExceptionHandler, ResponseStatus}
import org.springframework.web.context.request.async.AsyncRequestTimeoutException
import org.springframework.web.multipart.support.MissingServletRequestPartException
import org.springframework.web.servlet.NoHandlerFoundException

import scalapb.json4s.JsonFormatException

@ControllerAdvice
@Component
class GlobalExceptionHandler {

  @ExceptionHandler(Array(classOf[NotFoundException]))
  def handleNotFoundException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.NOT_FOUND)

  @ExceptionHandler(Array(classOf[TimeoutException]))
  def handleTimeoutException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.INTERNAL_SERVER_ERROR)

  @ExceptionHandler(Array(classOf[InvalidActorNameException]))
  def handleBundleException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[JsonProcessingException], classOf[JsonFormatException]))
  def handleJsonParseException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  // returning the same status code as SpringBoot for Spring-related exceptions
  @ExceptionHandler(Array(classOf[HttpRequestMethodNotSupportedException]))
  def handleHttpRequestMethodNotSupportedException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.METHOD_NOT_ALLOWED)

  @ExceptionHandler(Array(classOf[HttpMediaTypeNotSupportedException]))
  def handleHttpMediaTypeNotSupportedException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.UNSUPPORTED_MEDIA_TYPE)

  @ExceptionHandler(Array(classOf[HttpMediaTypeNotAcceptableException]))
  def handleHttpMediaTypeNotAcceptableException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.NOT_ACCEPTABLE)

  @ExceptionHandler(Array(classOf[MissingPathVariableException]))
  def handleMissingPathVariableException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.INTERNAL_SERVER_ERROR)

  @ExceptionHandler(Array(classOf[MissingServletRequestParameterException]))
  def handleMissingServletRequestParameterException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.INTERNAL_SERVER_ERROR)

  @ExceptionHandler(Array(classOf[ServletRequestBindingException]))
  def handleServletRequestBindingException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[ConversionNotSupportedException]))
  def handleConversionNotSupportedException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.INTERNAL_SERVER_ERROR)

  @ExceptionHandler(Array(classOf[TypeMismatchException]))
  def handleTypeMismatchException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[HttpMessageNotReadableException]))
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  def handleHttpMessageNotReadableException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[HttpMessageNotWritableException]))
  def handleHttpMessageNotWritableException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.INTERNAL_SERVER_ERROR)

  @ExceptionHandler(Array(classOf[MethodArgumentNotValidException]))
  def handleMethodArgumentNotValidException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[MissingServletRequestPartException]))
  def handleMissingServletRequestPartException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[BindException]))
  def handleBindException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[NoHandlerFoundException]))
  def handleNoHandlerFoundException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.NOT_FOUND)

  @ExceptionHandler(Array(classOf[AsyncRequestTimeoutException]))
  def handleAsyncRequestTimeoutException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.SERVICE_UNAVAILABLE)

  private def errorResponse(ex: Exception, status: HttpStatus): ResponseEntity[Unit] = {
    GlobalExceptionHandler.logger.warn("Returned error due to ", ex)
    ResponseEntity.status(status).build[Unit]()
  }
}

object GlobalExceptionHandler {
  val logger = LoggerFactory.getLogger(classOf[GlobalExceptionHandler])
}