package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark

object Day31SQLite extends App {
  println("CH9: Data Sources - SQL")

  //SQL Databases
  //SQL datasources are one of the more powerful connectors because there are a variety of systems
  //to which you can connect (as long as that system speaks SQL). For instance you can connect to a
  //MySQL database, a PostgreSQL database, or an Oracle database. You also can connect to
  //SQLite, which is what we’ll do in this example. Of course, databases aren’t just a set of raw files,
  //so there are more options to consider regarding how you connect to the database. Namely you’re
  //going to need to begin considering things like authentication and connectivity (you’ll need to
  //determine whether the network of your Spark cluster is connected to the network of your
  //database system).
  //To avoid the distraction of setting up a database for the purposes of this book, we provide a
  //reference sample that runs on SQLite. We can skip a lot of these details by using SQLite,
  //because it can work with minimal setup on your local machine with the limitation of not being
  //able to work in a distributed setting. If you want to work through these examples in a distributed
  //setting, you’ll want to connect to another kind of database

  val spark = getSpark("Sparky")

  // in Scala
  val driver = "org.sqlite.JDBC"
  val path = "src/resources/flight-data/jdbc/my-sqlite.db"
  val url = s"jdbc:sqlite:${path}" //for other SQL databases you would add username and authentification here
  val tableName = "flight_info"

  //After you have defined the connection properties, you can test your connection to the database
  //itself to ensure that it is functional. This is an excellent troubleshooting technique to confirm that
  //your database is available to (at the very least) the Spark driver. This is much less relevant for
  //SQLite because that is a file on your machine but if you were using something like MySQL, you
  //could test the connection with the following

//just a test of our SQLite connection - just regular Scala code without Spark
//  val connection = DriverManager.getConnection(url)
//  println("SQL connection isClosed:", connection.isClosed())
//  connection.close()

  //If this connection succeeds, you’re good to go. Let’s go ahead and read the DataFrame from the
  //SQL table:

  // in Scala
  val dbDataFrame = spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", tableName) //we are loading a single table into a dataframe
    .option("driver", driver) //just a string with the type of database Driver we are using - here jdbc which is most popular
    .load()

  dbDataFrame.printSchema()
  dbDataFrame.describe().show()
  dbDataFrame.show(5, truncate = false)

  //As we create this DataFrame, it is no different from any other: you can query it, transform it, and
  //join it without issue. You’ll also notice that there is already a schema, as well. That’s because
  //Spark gathers this information from the table itself and maps the types to Spark data types. Let’s
  //get only the distinct locations to verify that we can query it as expected:

  dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)

  //this is optional and could be faster - basically you would ask the database SQL engine to do more work
  //and spark engine to do less

  //Spark can’t translate all of its own functions into the functions available in the SQL database in
  //which you’re working. Therefore, sometimes you’re going to want to pass an entire query into
  //your SQL that will return the results as a DataFrame. Now, this might seem like it’s a bit
  //complicated, but it’s actually quite straightforward. Rather than specifying a table name, you just
  //specify a SQL query. Of course, you do need to specify this in a special way; you must wrap the
  //query in parenthesis and rename it to something—in this case, I just gave it the same table name:

  // in Scala
  val pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
AS flight_info"""
  val dbDataFrameFromQuery = spark.read.format("jdbc")
    .option("url", url).option("dbtable", pushdownQuery).option("driver", driver)
    .load()

  //Now when you query this table, you’ll actually be querying the results of that query. We can see
  //this in the explain plan. Spark doesn’t even know about the actual schema of the table, just the
  //one that results from our previous query:

  println(dbDataFrameFromQuery.explain())

  //Writing to SQL Databases
  //Writing out to SQL databases is just as easy as before. You simply specify the URI and write out
  //the data according to the specified write mode that you want. In the following example, we
  //specify overwrite, which overwrites the entire table. We’ll use the CSV DataFrame that we
  //defined earlier in order to do this:

  dbDataFrameFromQuery.show(10)

  // in Scala
  val newPath = "jdbc:sqlite:src/resources/tmp/my-sqlite.db"

  val props = new java.util.Properties
  props.setProperty("driver", "org.sqlite.JDBC")

  dbDataFrameFromQuery //before write you could filter do some aggregartion etc
    .write
    .mode("overwrite")
    .jdbc(newPath, tableName, props)

  //TODO how would you add extra tables to already existing SQL database - probably involves append
  //documentation is here:
  //https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

  //Text Files
  //Spark also allows you to read in plain-text files. Each line in the file becomes a record in the
  //DataFrame. It is then up to you to transform it accordingly. As an example of how you would do
  //this, suppose that you need to parse some Apache log files to some more structured format, or
  //perhaps you want to parse some plain text for natural-language processing. Text files make a
  //great argument for the Dataset API due to its ability to take advantage of the flexibility of native
  //types.

  //Reading Text Files
  //Reading text files is straightforward: you simply specify the type to be textFile. With
  //textFile, partitioned directory names are ignored. To read and write text files according to
  //partitions, you should use text, which respects partitioning on reading and writing

  spark.read
    .textFile("src/resources/flight-data/csv/2010-summary.csv") //csv is technically a text file
    //by default each row will have a value column with string for that row
    //    .selectExpr("split(value, ',') as rows")
    .show(15)

  //so it is just rows of strings - it is up to us to transform them

  val dfFromText =   spark.read
    .textFile("src/resources/flight-data/csv/2010-summary.csv") //csv is technically a text file
    .selectExpr("split(value, ',') as rows")

  dfFromText.show(5)
  dfFromText.printSchema()

  //again reading .csv should be done with csv fromat

  //reading text should be reserved for those times when we do not have well structured data

  //Writing Text Files
  //When you write a text file, you need to be sure to have only one string column; otherwise, the
  //write will fail:

  dbDataFrame.select("ORIGIN_COUNTRY_NAME") //we select a single string column
    .write
    .text("src/resources/tmp/simple-text-file.txt")


  //Advanced I/O Concepts
  //We saw previously that we can control the parallelism of files that we write by controlling the
  //partitions prior to writing. We can also control specific data layout by controlling two things:
  //bucketing and partitioning (discussed momentarily).
  //Splittable File Types and Compression
  //Certain file formats are fundamentally “splittable.” This can improve speed because it makes it
  //possible for Spark to avoid reading an entire file, and access only the parts of the file necessary
  //to satisfy your query. Additionally if you’re using something like Hadoop Distributed File
  //System (HDFS), splitting a file can provide further optimization if that file spans multiple
  //blocks. In conjunction with this is a need to manage compression. Not all compression schemes
  //are splittable. How you store your data is of immense consequence when it comes to making
  //your Spark jobs run smoothly. We recommend Parquet with gzip compression.



}
