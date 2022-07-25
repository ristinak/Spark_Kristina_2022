package com.github.ristinak

import org.apache.spark.sql.SparkSession

object SparkUtil {
  /**
   * Returns a new or an existing Spark session
   * @param appName - name of our Spark instance
   * @param partitionCount default 5 - starting default is 200
   * @param master default "local"  - master URL to connect
   * @param verbose - prints debug info
   * @return sparkSession
   */
  def getSpark(appName:String, partitionCount:Int = 5, master:String = "local", verbose:Boolean = true): SparkSession = {
    if (verbose) println(s"$appName with Scala version: ${util.Properties.versionNumberString}")
    val sparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()
    sparkSession.conf.set("spark.sql.shuffle.partitions", partitionCount)
    if (verbose) println(s"Session started on Spark version ${sparkSession.version} with ${partitionCount} partitions")
    sparkSession
  }
}

