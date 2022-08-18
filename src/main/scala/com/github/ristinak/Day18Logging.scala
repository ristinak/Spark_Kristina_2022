package com.github.ristinak

import org.apache.log4j.Logger
import org.apache.logging.log4j.{Level}

//More about log4j Configuration
//https://logging.apache.org/log4j/2.x/manual/configuration.html

object Day18Logging extends App {
  //logging in JVM world is done through log4j library
  println(classOf[Day18Logging].getName)
//  val log = Logger.getLogger(classOf[Day18Logging].getName) //considered a good practice to assign classname to particular
  //so instead of println you would use this log.method
  //https://logging.apache.org/log4j/2.x/manual/migration.html below is for log4j 2
  val log = org.apache.logging.log4j.LogManager.getLogger(classOf[Day18Logging].getName) //considered a good practice to assign classname to particular
  //so instead of println you would use this log.method
  log.debug("Hello this is a debug message")
  log.info("Hello this is an info message")
  log.warn("This is a warning")
  log.error("This is an error!")
  //there are more wrapper libraries such as Logback
  //https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
}

class Day18Logging //empty class just to give name to our Logger, could have used a string
