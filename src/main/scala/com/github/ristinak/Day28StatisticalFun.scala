package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, corr, covar_pop, covar_samp, expr, kurtosis, lit, mean, skewness, stddev_pop, stddev_samp, var_pop, var_samp}

object Day28StatisticalFun extends App {
  println("Ch7: Statistics Functions")
  val spark = getSpark("Sparky")

  val numDF = spark.range(10).toDF("nums")
  numDF.show()

  //Variance and Standard Deviation
  //Calculating the mean naturally brings up questions about the variance and standard deviation.
  //These are both measures of the spread of the data around the mean. The variance is the average
  //of the squared differences from the mean, and the standard deviation is the square root of the
  //variance. You can calculate these in Spark by using their respective functions. However,
  //something to note is that Spark has both the formula for the sample standard deviation as well as
  //the formula for the population standard deviation. These are fundamentally different statistical
  //formulae, and we need to differentiate between them. By default, Spark performs the formula for
  //the sample standard deviation or variance if you use the variance or stddev functions.
  //You can also specify these explicitly or refer to the population standard deviation or variance

  val avg = numDF.selectExpr("avg(nums)").collect().head.getDouble(0)
  println(avg)
  //FIXME find an easier way to assign average to all columns

  val distDF = numDF.withColumn("nmean", lit(avg))
    .withColumn("distance", expr("nums - nmean"))
    .withColumn("distSquared", expr("distance*distance"))

  distDF.createOrReplaceTempView("dfTable")

  distDF.show()

  //regular variance
  distDF.selectExpr("sum(distSquared)/count(distSquared) as variance")
    .withColumn("stdDev", expr("sqrt(variance)"))
    .show()

  //sample variance
  distDF.selectExpr("sum(distSquared)/(count(distSquared)-1) as sampleVariance")
    .withColumn("stdDevSample", expr("sqrt(sampleVariance)"))
    .show()


  numDF.select(
    mean("nums"),
    var_pop("nums"),
    var_samp("nums"),
    stddev_pop("nums"),
    stddev_samp("nums"))
    .show()


  //skewness and kurtosis
  //Skewness and kurtosis are both measurements of extreme points in your data. Skewness
  //measures the asymmetry of the values in your data around the mean, whereas kurtosis is a
  //measure of the tail of data. These are both relevant specifically when modeling your data as a
  //probability distribution of a random variable. Although here we won’t go into the math behind
  //these specifically, you can look up definitions quite easily on the internet. You can calculate
  //these by using the functions:
  //https://en.wikipedia.org/wiki/Skewness
  //https://en.wikipedia.org/wiki/Kurtosis

  numDF.select(skewness("nums"), kurtosis("nums")).show()

  spark.sql(
    """
      |SELECT skewness(nums), kurtosis(nums) FROM dfTable
      |""".stripMargin)
    .show()
  //skewness should be 0 because well the data is perfectly centered around mean

  //https://en.wikipedia.org/wiki/Correlation

  //Covariance and Correlation
  //We discussed single column aggregations, but some functions compare the interactions of the
  //values in two difference columns together. Two of these functions are cov and corr, for
  //covariance and correlation, respectively. Correlation measures the Pearson correlation
  //coefficient, which is scaled between –1 and +1. The covariance is scaled according to the inputs
  //in the data.
  //Like the var function, covariance can be calculated either as the sample covariance or the
  //population covariance. Therefore it can be important to specify which formula you want to use.
  //Correlation has no notion of this and therefore does not have calculations for population or
  //sample. Here’s how they work:

  val df = readDataWithView(spark, "src/resources/retail-data/by-day/2010-12-01.csv")
  //notice by using above function we redefined view called dfTable (which is the default for the above function

  df.select(corr("InvoiceNo", "Quantity"),
    covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity"))
    .show()

  //it would be strange indeed if invoice number had some noticeable correlation to quantity of items ordered
  //it could be that later customers buy bigger batches(maybe different promotion or different sales team)
  //it is not completely out of question
  //still correlation does not imply causality
  //https://en.wikipedia.org/wiki/Correlation_does_not_imply_causation#:~:text=The%20phrase%20%22correlation%20does%20not,association%20or%20correlation%20between%20them.
  //but it "winks" and says look here something is interesting
  //again beware of "wet streets cause rain" fallacies :)
  //https://www.tylervigen.com/spurious-correlations

  //Aggregating to Complex Types
  //In Spark, you can perform aggregations not just of numerical values using formulas, you can also
  //perform them on complex types. For example, we can collect a list of values present in a given
  //column or only the unique values by collecting to a set.
  //You can use this to carry out some more programmatic access later on in the pipeline or pass the
  //entire collection in a user-defined function (UDF)

  //so set will be uniques
  //list will be everything from some row
  df.agg(collect_set("Country"), collect_list("Country")).show(truncate=false)

  spark.sql(
    """
      |SELECT collect_set(Country), collect_list(Country) FROM dfTable
      |""".stripMargin)
    .show(false)

  //TODO
  //load March 8th of 2011 CSV
  //lets show avg, variance, std, skew, kurtosis, correlation and population covariance

  //TODO transform unique Countries for that day into a regular Scala Array of strings

  //you could use SQL distinct of course - do not hav eto use collect_set but you can :)


}

