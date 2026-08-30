package com.onyx.foss.kotlin

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class OnyxKotlinApplication

fun main(args: Array<String>) {
    runApplication<OnyxKotlinApplication>(*args)
}
