package ml.combust.mleap.springboot

import javax.servlet.http.HttpServletRequest

import akka.pattern.AskTimeoutException
import com.fasterxml.jackson.core.JsonParseException
import ml.combust.mleap.executor.repository.BundleException
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

@ControllerAdvice
@Component
class GlobalExceptionHandler {

  @ExceptionHandler(Array(classOf[BundleException]))
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  def handleBundleException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[AskTimeoutException]))
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  def handleAskTimeoutException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.INTERNAL_SERVER_ERROR)

  // returning the same status code as SpringBoot for Spring-related exceptions
  @ExceptionHandler(Array(classOf[HttpRequestMethodNotSupportedException]))
  @ResponseStatus(HttpStatus.METHOD_NOT_ALLOWED)
  def handleHttpRequestMethodNotSupportedException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.METHOD_NOT_ALLOWED)

  @ExceptionHandler(Array(classOf[HttpMediaTypeNotSupportedException]))
  @ResponseStatus(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
  def handleHttpMediaTypeNotSupportedException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.UNSUPPORTED_MEDIA_TYPE)

  @ExceptionHandler(Array(classOf[HttpMediaTypeNotAcceptableException]))
  @ResponseStatus(HttpStatus.NOT_ACCEPTABLE)
  def handleHttpMediaTypeNotAcceptableException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.NOT_ACCEPTABLE)

  @ExceptionHandler(Array(classOf[MissingPathVariableException]))
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  def handleMissingPathVariableException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.INTERNAL_SERVER_ERROR)

  @ExceptionHandler(Array(classOf[MissingServletRequestParameterException]))
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  def handleMissingServletRequestParameterException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.INTERNAL_SERVER_ERROR)

  @ExceptionHandler(Array(classOf[ServletRequestBindingException]))
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  def handleServletRequestBindingException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[ConversionNotSupportedException]))
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  def handleConversionNotSupportedException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.INTERNAL_SERVER_ERROR)

  @ExceptionHandler(Array(classOf[TypeMismatchException]))
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  def handleTypeMismatchException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[HttpMessageNotReadableException]))
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  def handleHttpMessageNotReadableException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[HttpMessageNotWritableException]))
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  def handleHttpMessageNotWritableException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.INTERNAL_SERVER_ERROR)

  @ExceptionHandler(Array(classOf[MethodArgumentNotValidException]))
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  def handleMethodArgumentNotValidException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[MissingServletRequestPartException]))
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  def handleMissingServletRequestPartException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[BindException]))
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  def handleBindException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  @ExceptionHandler(Array(classOf[NoHandlerFoundException]))
  @ResponseStatus(HttpStatus.NOT_FOUND)
  def handleNoHandlerFoundException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.NOT_FOUND)

  @ExceptionHandler(Array(classOf[AsyncRequestTimeoutException]))
  @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
  def handleAsyncRequestTimeoutException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.SERVICE_UNAVAILABLE)

  @ExceptionHandler(Array(classOf[JsonParseException]))
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  def handleJsonParseException(req: HttpServletRequest, ex: Exception): ResponseEntity[Unit] =
    errorResponse(ex, HttpStatus.BAD_REQUEST)

  private def errorResponse(ex: Exception, status: HttpStatus): ResponseEntity[Unit] = {
    GlobalExceptionHandler.logger.warn("Returned error due to ", ex)
    ResponseEntity.status(status).build[Unit]()
  }
}

object GlobalExceptionHandler {
  val logger = LoggerFactory.getLogger(classOf[GlobalExceptionHandler])
}