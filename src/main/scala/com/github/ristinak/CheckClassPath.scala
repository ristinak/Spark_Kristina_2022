package com.github.ristinak

object CheckClassPath extends App {

//  I think this should work:
//
//  import java.lang.ClassLoader
  //https://stackoverflow.com/questions/30512598/spark-is-there-a-way-to-print-out-classpath-of-both-spark-shell-and-spark
  val cl = ClassLoader.getSystemClassLoader
  cl.asInstanceOf[java.net.URLClassLoader].getURLs.foreach(println)
}
