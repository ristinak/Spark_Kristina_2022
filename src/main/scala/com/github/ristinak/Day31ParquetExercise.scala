package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark

object Day31ParquetExercise extends App {

  val spark = getSpark("Sparky")

  val dfParquet = spark.read.format("parquet")
    .load("src/resources/regression")

  dfParquet.printSchema()
  dfParquet.show(5)
  dfParquet.describe().show()

}
