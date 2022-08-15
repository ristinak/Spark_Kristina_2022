package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark

object Day31Parquet extends App {
  val spark = getSpark("Sparky")

  //Parquet Files
  //Parquet is an open source column-oriented data store that provides a variety of storage
  //optimizations, especially for analytics workloads. It provides columnar compression, which
  //saves storage space and allows for reading individual columns instead of entire files. It is a file
  //format that works exceptionally well with Apache Spark and is in fact the default file format. We
  //recommend writing data out to Parquet for long-term storage because reading from a Parquet file
  //will always be more efficient than JSON or CSV. Another advantage of Parquet is that it
  //supports complex types. This means that if your column is an array (which would fail with a
  //CSV file, for example), map, or struct, you’ll still be able to read and write that file without
  //issue. Here’s how to specify Parquet as the read format:
  //spark.read.format("parquet")

  //Reading Parquet Files
  //Parquet has very few options because it enforces its own schema when storing data. Thus, all you
  //need to set is the format and you are good to go. We can set the schema if we have strict
  //requirements for what our DataFrame should look like. Oftentimes this is not necessary because
  //we can use schema on read, which is similar to the inferSchema with CSV files. However, with
  //Parquet files, this method is more powerful because the schema is built into the file itself (so no
  //inference needed).
  //Here are some simple examples reading from parquet:

  val df = spark.read.format("parquet")
//    .load("src/resources/flight-data/parquet/2010-summary.parquet")
    //version mismatch generates warnings - creator metadata not preserved
    //https://stackoverflow.com/questions/42320157/warnings-trying-to-read-spark-1-6-x-parquet-into-spark-2-x
    .load("src/resources/flight-data/parquet/2010-summary_fixed.parquet")

  df.show(5)
  df.describe().show()
  df.printSchema()

//so we will save using our current parquet standard, we read from the old one with some warnings
//  df.write
//    .format("parquet")
//    .mode("overwrite") //we could use .option("mode", "overwrite") but harder to debug typos
//    .save("src/resources/flight-data/parquet/2010-summary_fixed.parquet")

  //TODO read parquet file from src/resources/regression
  //TODO print schema
  //TODO print a sample of some rows
  //TODO show some basic statistics - describe would be a good start
  //TODO if you encounter warning reading data THEN save into src/resources/regression_fixed

  val dfParquet = spark.read.format("parquet")
    .load("src/resources/regression")

  dfParquet.printSchema()
  dfParquet.show(5)
  dfParquet.describe().show()
}
