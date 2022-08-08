package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import com.github.ristinak.Util.myRound
import org.apache.spark.sql.functions.{col, udf}

object Day27UserDefinedFunctionsExercise extends App {
  println("Ch6: UDFs - User Defined Functions")

  val spark = getSpark("Sparky")
  val df = spark.range(-40, 121).toDF("temperatureF")

  def tempFtoC(n: Double):Double = myRound((n-32)*5/9, 2)
//  def tempFtoC(n: Double):Double = ((n-32)*5/9).round

  val tempFtoC = udf(tempFtoC(_:Double):Double)

  df
    .withColumn("temperatureC", tempFtoC(col("temperatureF")))
    .where(col("temperatureF") >= 90 && col("temperatureF") <= 110 )
    .show()

  spark.udf.register("tempFtoC", tempFtoC(_:Double):Double)
  df.createOrReplaceTempView("dfTable")

  spark.sql(
    """
      |SELECT *,
      |tempFtoC(temperatureF) temperatureC,
      |FROM dfTable
      |WHERE temperatureF >= 90 AND temperatureF <= 110
      |""".stripMargin)
    .show()

  //TODO create a UDF which converts Fahrenheit to Celsius
  // Create DF with column temperatureF with temperatures from -40 to 120 using range or something else if want
  // register your UDF function
  // use your UDF to create temperatureC column with the actual conversion

  // show both columns starting with F temperature at 90 and ending at 110( both included)

  //You probably want Double incoming and Double also as a return
}

