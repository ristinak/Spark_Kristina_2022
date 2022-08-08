package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.sql.functions.{col, udf}

object Day27UserDefinedFunctions extends App {
  println("Ch6: UDFs - User Defined Functions")
  val spark = getSpark("Sparky")

  val df = spark.range(10).toDF("num")
  df.printSchema()
  df.show()

  def power3(n: Double):Double = n*n*n //so you can make your own formula here that depends on a single variable
  def power3int(n: Long):Long = n*n*n
  println(power3(10)) //just a regular Scala function so far
  println(power3int(10)) //just a regular Scala function so far

  //Now that we’ve created these functions and tested them, we need to register them with Spark so
  //that we can use them on all of our worker machines. Spark will serialize the function on the
  //driver and transfer it over the network to all executor processes. This happens regardless of
  //language.
  //When you use the function, there are essentially two different things that occur. If the function is
  //written in Scala or Java, you can use it within the Java Virtual Machine (JVM). This means that
  //there will be little performance penalty aside from the fact that you can’t take advantage of code
  //generation capabilities that Spark has for built-in functions.

  //we
  //need to register the function to make it available as a DataFrame function

  //so names are up to you, should be meaningful of course
  val power3udf = udf(power3(_:Double):Double)
  val power3IntUdf = udf(power3int(_:Long):Long)

  df
    .withColumn("numCubed", power3udf(col("num")))
    .withColumn("numCubedInteger", power3IntUdf(col("num")))
    .show()

  //At this juncture, we can use this only as a DataFrame function. That is to say, we can’t use it
  //within a string expression, only on an expression. However, we can also register this UDF as a
  //Spark SQL function. This is valuable because it makes it simple to use this function within SQL
  //as well as across languages.
  //Let’s register the function in Scala:

  // in Scala
  spark.udf.register("power3", power3(_:Double):Double)
  df.selectExpr("power3(num)").show(5)

  //lets register our other function with integers
  spark.udf.register("power3int", power3int(_:Long):Long)

  df.createOrReplaceTempView("dfTable")

  spark.sql(
    """
      |SELECT *,
      |power3(num),
      |power3int(num)
      |FROM dfTable
      |""".stripMargin)
    .show()

  //TODO create a UDF which converts Fahrenheit to Celsius
  //TODO Create DF with column temperatureF with temperatures from -40 to 120 using range or something else if want
  //TODO register your UDF function
  //TODO use your UDF to create temperatureC column with the actual conversion

  //TODO show both columns starting with F temperature at 90 and ending at 110( both included)

  //You probably want Double incoming and Double also as a return
}
