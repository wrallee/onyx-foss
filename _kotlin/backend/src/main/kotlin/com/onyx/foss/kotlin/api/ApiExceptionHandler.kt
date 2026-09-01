package com.onyx.foss.kotlin.api

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

class ApiException(val status: HttpStatus, override val message: String) : RuntimeException(message)

@RestControllerAdvice
class ApiExceptionHandler {
    @ExceptionHandler(ApiException::class)
    fun apiError(error: ApiException): ResponseEntity<Map<String, String>> =
        ResponseEntity.status(error.status).body(mapOf("detail" to error.message))

    @ExceptionHandler(IllegalArgumentException::class)
    fun invalidInput(error: IllegalArgumentException): ResponseEntity<Map<String, String>> =
        ResponseEntity.badRequest().body(mapOf("detail" to (error.message ?: "Invalid request")))

    @ExceptionHandler(DataIntegrityViolationException::class)
    fun conflictingData(error: DataIntegrityViolationException): ResponseEntity<Map<String, String>> =
        ResponseEntity.status(HttpStatus.CONFLICT).body(mapOf("detail" to "Request conflicts with existing data"))
}
