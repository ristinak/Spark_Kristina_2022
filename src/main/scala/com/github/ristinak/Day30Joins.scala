package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark

object Day30Joins extends App {
  println("Ch8: Joins")
  val spark = getSpark("Sparky")

  //Join Expressions
  //A join brings together two sets of data, the left and the right, by comparing the value of one or
  //more keys of the left and right and evaluating the result of a join expression that determines
  //whether Spark should bring together the left set of data with the right set of data. The most
  //common join expression, an equi-join, compares whether the specified keys in your left and
  //right datasets are equal. If they are equal, Spark will combine the left and right datasets. The
  //opposite is true for keys that do not match; Spark discards the rows that do not have matching
  //keys. Spark also allows for much more sophsticated join policies in addition to equi-joins. We
  //can even use complex types and perform something like checking whether a key exists within an
  //array when you perform a join.

  //Join Types
  //Whereas the join expression determines whether two rows should join, the join type determines
  //what should be in the result set. There are a variety of different join types available in Spark for
  //you to use:
  //Inner joins (keep rows with keys that exist in the left and right datasets)
  //Outer joins (keep rows with keys in either the left or right datasets)
  //Left outer joins (keep rows with keys in the left dataset)
  //Right outer joins (keep rows with keys in the right dataset)
  //Left semi joins (keep the rows in the left, and only the left, dataset where the key
  //appears in the right dataset)
  //Left anti joins (keep the rows in the left, and only the left, dataset where they do not
  //appear in the right dataset)
  //Natural joins (perform a join by implicitly matching the columns between the two
  //datasets with the same names)
  //Cross (or Cartesian) joins (match every row in the left dataset with every row in the
  //right dataset)
  //If you have ever interacted with a relational database system, or even an Excel spreadsheet, the
  //concept of joining different datasets together should not be too abstract. Let’s move on to
  //showing examples of each join type. This will make it easy to understand exactly how you can
  //apply these to your own problems. To do this, let’s create some simple datasets that we can use
  //in our examples
  //some simple Datasets
  import spark.implicits._ // implicits will let us use toDF on simple sequence
  //regular Sequence does not have toDF method that why we had to use implicits from spark

  // in Scala
  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)),
    (3, "Valdis Saulespurens", 2, Seq(100,250)))
    .toDF("id", "name", "graduate_program", "spark_status")

  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")
  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")

  person.show()
  graduateProgram.show()
  sparkStatus.show()


  person.createOrReplaceTempView("person")
  graduateProgram.createOrReplaceTempView("graduateProgram")
  sparkStatus.createOrReplaceTempView("sparkStatus")

  //Inner Joins
  //Inner joins evaluate the keys in both of the DataFrames or tables and include (and join together)
  //only the rows that evaluate to true. In the following example, we join the graduateProgram
  //DataFrame with the person DataFrame to create a new DataFrame:

  // in Scala - notice the triple ===
  val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

  //Inner joins are the default join, so we just need to specify our left DataFrame and join the right in
  //the JOIN expression:
  person.join(graduateProgram, joinExpression).show()

  //same in spark sql
  spark.sql(
    """
      |SELECT * FROM person JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //TODO inner join src/resources/retail-data/all/online-retail-dataset.csv
  //TODO with src/resources/retail-data/customers.csv
  //on Customer ID in first matching Id in second

  //in other words I want to see the purchases of these customers with their full names
  //try to show it both spark API and spark SQL

}
