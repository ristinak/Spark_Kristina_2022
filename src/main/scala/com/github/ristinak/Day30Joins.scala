package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.sql.functions.expr

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

  //We can also specify this explicitly by passing in a third parameter, the joinType:

  //again no need to pass inner since it is the default
  person.join(graduateProgram, joinExpression, joinType = "inner").show()

  //Outer Joins
  //Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins
  //together) the rows that evaluate to true or false. If there is no equivalent row in either the left or
  //right DataFrame, Spark will insert null

  println("FULL OUTER JOIN")

  person.join(graduateProgram, joinExpression, "outer").show()

  spark.sql(
    """
      |SELECT * FROM person
      |FULL OUTER JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //so we should see some null values in an FULL OUTER JOIN

  //Left Outer Joins
  //Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from
  //the left DataFrame as well as any rows in the right DataFrame that have a match in the left
  //DataFrame. If there is no equivalent row in the right DataFrame, Spark will insert null

  println("LEFT OUTER JOINS")
  person.join(graduateProgram, joinExpression, "left_outer").show()
  spark.sql(
    """
      |SELECT * FROM person
      |LEFT OUTER JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //Right Outer Joins
  //Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows
  //from the right DataFrame as well as any rows in the left DataFrame that have a match in the right
  //DataFrame. If there is no equivalent row in the left DataFrame, Spark will insert null:

  println("RIGHT OUTER JOIN")

  person.join(graduateProgram, joinExpression, "right_outer").show()
  spark.sql(
    """
      |SELECT * FROM person
      |RIGHT OUTER JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()


  //Left Semi Joins
  //Semi joins are a bit of a departure from the other joins. They do not actually include any values
  //from the right DataFrame. They only compare values to see if the value exists in the second
  //DataFrame. If the value does exist, those rows will be kept in the result, even if there are
  //duplicate keys in the left DataFrame. Think of left semi joins as filters on a DataFrame, as
  //opposed to the function of a conventional join:

  println("LEFT SEMI JOIN")
  //notice we are using the same joinExpression but we start with graduateProgram here
  graduateProgram.join(person, joinExpression, "left_semi").show() //should show only programs with graduates
  person.join(graduateProgram, joinExpression, "left_semi").show() //should show only persons who attended a known university


  //we create a new dataFrame by using Union of two dataframes
  // in Scala
  //in this case the id is a duplicate of previous one
  //often you do not want this :)
  val gradProgram2 = graduateProgram.union(Seq(
    (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
  gradProgram2.createOrReplaceTempView("gradProgram2")

  gradProgram2.join(person, joinExpression, "left_semi").show()

  spark.sql(
    """
      |SELECT * FROM gradProgram2 LEFT SEMI JOIN person
      |ON gradProgram2.id = person.graduate_program
      |""".stripMargin)
    .show()

  // more on differences on INNER JOIN and LEFT SEMI JOIN
  //https://stackoverflow.com/questions/21738784/difference-between-inner-join-and-left-semi-join

  //Left Anti Joins
  //Left anti joins are the opposite of left semi joins. Like left semi joins, they do not actually
  //include any values from the right DataFrame. They only compare values to see if the value exists
  //in the second DataFrame. However, rather than keeping the values that exist in the second
  //DataFrame, they keep only the values that do not have a corresponding key in the second
  //DataFrame. Think of anti joins as a NOT IN SQL-style filter:

  println("LEFT ANTI JOIN")

  graduateProgram.join(person, joinExpression, "left_anti").show()

  spark.sql(
    """
      |SELECT * FROM graduateProgram
      |LEFT ANTI JOIN person
      |ON graduateProgram.id = person.graduate_program
      |""".stripMargin)
    .show()

  //so we should expect to see University of Latvia and Vilnius University since they do not have any known students here

  //similarly we should expect to see students without a matching university program if our left table is persons
  person.join(graduateProgram, joinExpression, "left_anti").show()

  //Natural Joins
  //Natural joins make implicit guesses at the columns on which you would like to join. It finds
  //matching columns and returns the results. Left, right, and outer natural joins are all supported.
  //WARNING: may not give the results you expect since spark engine tries to implicitly guess the right type of join
  //WARNING
  //Implicit is always dangerous! The following query will give us incorrect results because the two
  //DataFrames/tables share a column name (id), but it means different things in the datasets. You should
  //always use this join with caution.

  println("NATURAL JOIN :personally I would avoid due to possible implicit mistakes")
  spark.sql(
    """
      |SELECT * FROM graduateProgram
      |NATURAL JOIN person
      |""".stripMargin)
    .show() //will produce logically incorrect joins because id columns have different meanings in these tables

  //only time i could using natural joins would be if your columns are distinctive like personID, schoolId, etc

  //Cross (Cartesian) Joins
  //The last of our joins are cross-joins or cartesian products. Cross-joins in simplest terms are inner
  //joins that do not specify a predicate. Cross joins will join every single row in the left DataFrame
  //to ever single row in the right DataFrame. This will cause an absolute explosion in the number of
  //rows contained in the resulting DataFrame. If you have 1,000 rows in each DataFrame, the cross-
  //join of these will result in 1,000,000 (1,000 x 1,000) rows. For this reason, you must very
  //explicitly state that you want a cross-join by using the cross join keyword:

  //so we have 6 persons
  //and 5 graduate programs
  //we expect to see 5*6 = 30 rows in a cross (Cartesian) join

  println("CROSS (CARTESIAN) JOINS")
  //so technically joinExpression does no work here
  graduateProgram.join(person, joinExpression, "cross")
    .show(32)

  spark.sql(
    """
      |  SELECT * FROM graduateProgram
      |  CROSS JOIN person
      |  ON graduateProgram.id = person.graduate_program
      |""".stripMargin)
    .show(32)

  println("So we need to explictly call crossJoin or set spark.sql.crossJoin.enable  because cross joins produce such big results" )

  person.crossJoin(graduateProgram).show(32) //so will give person.rowcount * graduateProgram.rowcount results/rows
  spark.sql(
    """
      |SELECT * FROM graduateProgram
      |CROSS JOIN person
      |""".stripMargin)
    .show(32) //we are only getting 30 results


  spark.conf.set("spark.sql.crossJoin.enable", true) //TODO see if it is correct
  graduateProgram.join(person, joinExpression, "cross")
    .show(32)

  //Joins on Complex Types
  //Even though this might seem like a challenge, it’s actually not. Any expression is a valid join
  //expression, assuming that it returns a Boolean:
  println("JOINS on complex types")

  person.withColumnRenamed("id", "personId") //we rename the person id to avoid confusion with spark status id
    .join(sparkStatus, expr("array_contains(spark_status, id)")).show()
  //https://spark.apache.org/docs/latest/api/sql/index.html#array_contains

  spark.sql(
    """
      |SELECT * FROM
      |(select id as personId, name, graduate_program, spark_status FROM person)
      |INNER JOIN sparkStatus
      | ON array_contains(spark_status, id)
      |""".stripMargin)
    .show()

  //Handling Duplicate Column Names
  //One of the tricky things that come up in joins is dealing with duplicate column names in your
  //results DataFrame. In a DataFrame, each column has a unique ID within Spark’s SQL Engine,
  //Catalyst. This unique ID is purely internal and not something that you can directly reference.
  //This makes it quite difficult to refer to a specific column when you have a DataFrame with
  //duplicate column names.
  //This can occur in two distinct situations:
  //The join expression that you specify does not remove one key from one of the input
  //DataFrames and the keys have the same column name
  //Two columns on which you are not performing the join have the same name
  //Let’s create a problem dataset that we can use to illustrate these problems:
  //val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
  //val joinExpr = gradProgramDupe.col("graduate_program") === person.col(
  //"graduate_program")
  //Note that there are now two graduate_program columns, even though we joined on that key:
  //person.join(gradProgramDupe, joinExpr).show()
  //The challenge arises when we refer to one of these columns:
  //person.join(gradProgramDupe, joinExpr).select("graduate_program").show()
  //Given the previous code snippet, we will receive an error. In this particular example, Spark
  //generates this message:
  //org.apache.spark.sql.AnalysisException: Reference 'graduate_program' is
  //ambiguous, could be: graduate_program#40, graduate_program#1079.;
  //Approach 1: Different join expression
  //When you have two keys that have the same name, probably the easiest fix is to change the join
  //expression from a Boolean expression to a string or sequence. This automatically removes one of
  //the columns for you during the join:
  //person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()
  //Approach 2: Dropping the column after the join
  //Another approach is to drop the offending column after the join. When doing this, we need to
  //refer to the column via the original source DataFrame. We can do this if the join uses the same
  //key names or if the source DataFrames have columns that simply have the same name:
  //person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
  //.select("graduate_program").show()
  //val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
  //person.join(graduateProgram, joinExpr).drop(graduateProgram.col("id")).show()
  //This is an artifact of Spark’s SQL analysis process in which an explicitly referenced column will
  //pass analysis because Spark has no need to resolve the column. Notice how the column uses the
  //.col method instead of a column function. That allows us to implicitly specify that column by
  //its specific ID.
  //Approach 3: Renaming a column before the join
  //We can avoid this issue altogether if we rename one of our columns before the join:
  //val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
  //val joinExpr = person.col("graduate_program") === gradProgram3.col("grad_id")
  //person.join(gradProgram3, joinExpr).show()

  //my advice use option 3 - rename any possible conflicting columns


  //How Spark Performs Joins
  //To understand how Spark performs joins, you need to understand the two core resources at play:
  //the node-to-node communication strategy and per node computation strategy. These internals are
  //likely irrelevant to your business problem. However, comprehending how Spark performs joins
  //can mean the difference between a job that completes quickly and one that never completes at
  //all.
  //Communication Strategies
  //Spark approaches cluster communication in two different ways during joins. It either incurs a
  //shuffle join, which results in an all-to-all communication or a broadcast join. Keep in mind that
  //there is a lot more detail than we’re letting on at this point, and that’s intentional. Some of these
  //internal optimizations are likely to change over time with new improvements to the cost-based
  //optimizer and improved communication strategies. For this reason, we’re going to focus on the
  //high-level examples to help you understand exactly what’s going on in some of the more
  //common scenarios, and let you take advantage of some of the low-hanging fruit that you can use
  //right away to try to speed up some of your workloads.
  //The core foundation of our simplified view of joins is that in Spark you will have either a big
  //table or a small table. Although this is obviously a spectrum (and things do happen differently if
  //you have a “medium-sized table”), it can help to be binary about the distinction for the sake of
  //this explanation.
  //Big table–to–big table
  //When you join a big table to another big table, you end up with a shuffle join

  //In a shuffle join, every node talks to every other node and they share data according to which
  //node has a certain key or set of keys (on which you are joining). These joins are expensive
  //because the network can become congested with traffic, especially if your data is not partitioned
  //well

  //so worry about this when you experience slow speeds - it is possible that you might be able to avoid shuffle join

  //one way of optimizing

  //  The SQL interface also includes the ability to provide hints to perform joins. These are not
  //  enforced, however, so the optimizer might choose to ignore them. You can set one of these hints
  //  by using a special comment syntax. MAPJOIN, BROADCAST, and BROADCASTJOIN all do the same
  //    thing and are all supported:
  //    -- in SQL
  //  SELECT /*+ MAPJOIN(graduateProgram) */ * FROM person JOIN graduateProgram
  //    ON person.graduate_program = graduateProgram.id
  //  This doesn’t come for free either: if you try to broadcast something too large, you can crash your
  //    driver node (because that collect is expensive). This is likely an area for optimization in the
  //  future.

  //keep up with the latest hints here:
  //https://spark.apache.org/docs/3.0.0/sql-ref-syntax-qry-select-hints.html

  //Conclusion
  //In this chapter, we discussed joins, probably one of the most common use cases. One thing we
  //did not mention but is important to consider is if you partition your data correctly prior to a join,
  //you can end up with much more efficient execution because even if a shuffle is planned, if data
  //from two different DataFrames is already located on the same machine, Spark can avoid the
  //shuffle. Experiment with some of your data and try partitioning beforehand to see if you can
  //notice the increase in speed when performing those joins



}
