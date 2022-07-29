package com.github.ristinak

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{asc, col, desc, expr}

import scala.util.Random

object Day22MoreColumnTransformations extends App {
  println("Ch5: Column, Row operations")
  val spark = SparkUtil.getSpark("BasicSpark")
  //so this will set SQL sensitivity to be case sensitive
  spark.conf.set("spark.sql.caseSensitive", true)
  //so from now on the SQL queries would be case sensitive

  //there are many configuration settings you can set at runtime using the above syntax
  //https://spark.apache.org/docs/latest/configuration.html

  //  val flightPath = "src/resources/flight-data/json/2015-summary.json"
  val flightPath = "src/resources/flight-data/json/2014-summary.json"

  //so automatic detection of schema
  val df = spark.read.format("json")
    .load(flightPath)

  df.show(5)

  //Removing Columns
  //Now that we’ve created this column, let’s take a look at how we can remove columns from
  //DataFrames. You likely already noticed that we can do this by using select. However, there is
  //also a dedicated method called drop

  //will show dataframe with the two columns below dropped
  df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(5)

  //we can use select to get columns we want or we can use drop to remove columns we do not want

  df.printSchema()
  //our count is already long
  //so will cast to integer so half the size of long
  //let's cast to double which is floating point just double precision of regular float

  val dfWith3Counts = df.withColumn("count2", col("count").cast("int"))
    .withColumn("count3", col("count").cast("double"))

  dfWith3Counts.show(5)
  dfWith3Counts.printSchema()

  //most often the cast would be from string to int, long or double
  //reason being that you want to peform some numeric calculation on that column

  //Filtering Rows
  //To filter rows, we create an expression that evaluates to true or false. You then filter out the rows
  //with an expression that is equal to false. The most common way to do this with DataFrames is to
  //create either an expression as a String or build an expression by using a set of column
  //manipulations. There are two methods to perform this operation: you can use where or filter
  //and they both will perform the same operation and accept the same argument types when used
  //with DataFrames. We will stick to where because of its familiarity to SQL; however, filter is
  //valid as well

  df.filter(col("count") < 2).show(5) //less used
  df.where("count < 2").show(5) //more common due to being similar to SQL syntax

  //Instinctually, you might want to put multiple filters into the same expression. Although this is
  //possible, it is not always useful, because Spark automatically performs all filtering operations at
  //the same time regardless of the filter ordering. This means that if you want to specify multiple
  //AND filters, just chain them sequentially and let Spark handle the rest:

  df.where("count > 5 AND count < 10").show(5) //works but better to chain multiple filters for optimization

  //prefer multiple chains
  df.where("count > 5")
    .where("count < 10")
    .show(5)

  // in Scala
  df.where(col("count") < 2)
    .where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
    .show(3)

  //Getting Unique Rows
  //A very common use case is to extract the unique or distinct values in a DataFrame. These values
  //can be in one or more columns. The way we do this is by using the distinct method on a
  //DataFrame, which allows us to deduplicate any rows that are in that DataFrame. For instance,
  //let’s get the unique origins in our dataset. This, of course, is a transformation that will return a
  //new DataFrame with only unique rows:

  val countUniqueFlights = df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
  println(s"Number of unique flights ${countUniqueFlights}")

  println(df.select("ORIGIN_COUNTRY_NAME").distinct().count()) //should be 125 unique/distinct origin countries

  //Random Samples
  //Sometimes, you might just want to sample some random records from your DataFrame. You can
  //do this by using the sample method on a DataFrame, which makes it possible for you to specify
  //a fraction of rows to extract from a DataFrame and whether you’d like to sample with or without
  //replacement:

  //  val seed = 42 //so static seed should guarantte same sample each time
  //  val seed = Random.nextInt() //so up to 4 billion different integers
  //  val withReplacement = false
  //  //if you set withReplacement true, that means you will be putting your row sampled back into the cookie jar
  //  //https://stackoverflow.com/questions/53689047/what-does-withreplacement-do-if-specified-for-sample-against-a-spark-dataframe
  //  //usually you do not want to draw the same ticket more than once
  //  val fraction = 0.1 //so 10 % is just a rough estimate, for smaller datasets such as ours
  //  //Note:
  //  //This is NOT guaranteed to provide exactly the fraction of the count of the given Dataset.
  //
  //  val dfSample = df.sample(withReplacement, fraction, seed)
  //  dfSample.show(5)
  //  println(s"We got ${dfSample.count()} samples")

  // in Scala
  //we get Array of Dataset[Row] which is same as Array[DataFrame]
  //so split 25 percent and 75 percent roughly again not exact!
  //  val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
  //
  //  for ((dFrame, i) <- dataFrames.zipWithIndex) { //we could have used df or anything else instead of dFrame
  //    println(s"DataFrame No. $i has ${dFrame.count} rows")
  //  }
  //
  //  def getDataFrameStats(dFrames:Array[Dataset[Row]], df:DataFrame): Array[Long] = {
  //    dFrames.map(d => d.count() * 100 / df.count())
  //  }
  //
  //  val dPercentages= getDataFrameStats(dataFrames, df)
  //  println("DataFrame percentages")
  //  dPercentages.foreach(println)

  //so now the proportion should be roughly 2/5 to the first dataframe and 3/5 to the 2nd
  //so randomSplit will normalize 2,3 to 0.4, 0.6
  val dFrames23split = df.randomSplit(Array(2, 3), seed)
  getDataFrameStats(dFrames23split, df).foreach(println)

  //TODO open up 2014-summary.json file

  //TODO Task 1 - Filter only flights FROM US that happened more than 10 times
  // TODO filter flights FROM US that happened more than 10 times

  df.where("ORIGIN_COUNTRY_NAME = 'United States'")
    .where("count > 10")
    .show()

  //TODO Task 2 - I want a random sample from all 2014 of roughly 30 percent, you can use a fixed seed
  //subtask I want to see the actual row count
  println(s"We have ${df.count()} rows/records in 2014")
  val seed = 5 //can use random number if you want random drawing, specific seed will be fixed each time
  val withReplacement = false //we do not put the samples back into the cookie jar/lotto jar
  val fraction = 0.3 //so 30% ?

  //neither of these 2 approaches seem to help
  //  df.cache() //so we will try caching the values
  //  //suggestion from https://medium.com/udemy-engineering/pyspark-under-the-hood-randomsplit-and-sample-inconsistencies-examined-7c6ec62644bc
  val dfRepartition = df.repartition(1, col("count")) //single partition DF

  val dfSample = dfRepartition.sample(withReplacement, fraction, seed)

  dfSample.show(5)
  println(s"We got ${dfSample.count()} samples")
  val dfSamplePercentage = dfSample.count()*100.0 / df.count()
  println(s"Actual percentage is $dfSamplePercentage")

  //TODO Task 3 - I want a split of full 2014 dataframe into 3 Dataframes with the following proportions 2,9, 5
  //subtask I want to see the row count for these dataframes and percentages

  //TODO Task 3 - I want a split of full 2014 dataframe into 3 Dataframes with the following proportions 2,9, 5
  val dataFrames = dfRepartition.randomSplit(Array(2,5,9), seed)

  def getDataFrameStats(dFrames:Array[Dataset[Row]], df:DataFrame): Array[Double] = {
    dFrames.map(d => d.count() * 100.0 / df.count())
  }
  println("Percentages for 2/5/9 split so 16 total parts")
  println(s"Expected percentages ${2.0*100/16} / ${5.0*100/16} / ${9.0*100/16}")
  getDataFrameStats(dataFrames, df).foreach(println)
  //subtask I want to see the row count for these dataframes and percentages

  for ((dFrame, i) <- dataFrames.zipWithIndex) {
    (println(s"$i dataframe consists of ${dFrame.count} rows"))
  }

  //Concatenating and Appending Rows (Union)
  //As you learned in the previous section, DataFrames are immutable. This means users cannot
  //append to DataFrames because that would be changing it. To append to a DataFrame, you must
  //union the original DataFrame along with the new DataFrame. This just concatenates the two
  //DataFramess. To union two DataFrames, you must be sure that they have the same schema and
  //number of columns; otherwise, the union will fail.

  val unionFirstTwo = dataFrames.head.union(dataFrames(1)) //so union of first and second from our split
  unionFirstTwo.show(5)
  println(s"Size of Union of two Dataframes is ${unionFirstTwo.count()}")


  //we can create some Rows -> DF by hand like before
  val schema = df.schema //so we save a copy of original dataframe schema
  df.printSchema()
  //now all we need to do is match the column data types
  val newRows = Seq(
    Row("New Country", "Other Country", 5L), //so long because that is what schema demands
    Row("New Country 2", "Other Country 3", 1L)
  )
  val parallelizedRows = spark.sparkContext.parallelize(newRows)
  val newDF = spark.createDataFrame(parallelizedRows, schema)

  df.union(newDF) //so our unionized dataframe is ready here - we are not saving it
    .where("count = 1")
    .where(col("ORIGIN_COUNTRY_NAME") =!= "United States")
    .show()

  //Sorting Rows
  //When we sort the values in a DataFrame, we always want to sort with either the largest or
  //smallest values at the top of a DataFrame. There are two equivalent operations to do this sort
  //and orderBy that work the exact same way. They accept both column expressions and strings as
  //well as multiple columns. The default is to sort in ascending orde

  df.sort("count").show(5)
  //we have a lot of flight counts with value 1 so we could use a 2nd tiebreak
  //so when you have two columns 2nd one is the tiebreak so here alphabetical
  df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
  df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5) //this one I would skip

  //To more explicitly specify sort direction, you need to use the asc and desc functions if operating
  //on a column. These allow you to specify the order in which a given column should be sorted

  df.orderBy(expr("count desc")).show(5) //FIXME so here desc modifier is not working
  //good riddance if it does not work
  df.orderBy(desc("count")).show(5)
  //so since descending counts are different at the beginning onlly the tiny counts (1 or 2 wor say 5 would have tiebreaks)
  //you would expect all 3 of these to be exactly the same
  df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(5)
  //you can add more sorting columns(more tiebreaks)
}
