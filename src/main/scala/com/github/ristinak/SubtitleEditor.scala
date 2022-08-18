package com.github.ristinak

import scala.collection.mutable.ArrayBuffer

object SubtitleEditor extends App {

  val source = "src/resources/TELL ME LT.txt"
  val lines = Util.getLinesFromFile(source)

  val cleanLines = lines.filter(line => !line.contains('['))

  println(cleanLines.mkString("\n").take(100))

  val destPath = "src/resources/Tell me clean LT.txt"
  Util.saveLines(destPath, cleanLines)

}
