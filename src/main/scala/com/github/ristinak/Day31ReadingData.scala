package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Day31ReadingData extends App {
  println("CH9: Data Sources Reading / Writing")
  val spark = getSpark("Sparky")

  //CSV Files
  //CSV stands for commma-separated values. This is a common text file format in which each line
  //represents a single record, and commas separate each field within a record. CSV files, while
  //seeming well structured, are actually one of the trickiest file formats you will encounter because
  //not many assumptions can be made in production scenarios about what they contain or how they
  //are structured. For this reason, the CSV reader has a large number of options. These options give
  //you the ability to work around issues like certain characters needing to be escaped—for example,
  //commas inside of columns when the file is also comma-delimited or null values labeled in an
  //unconventional way.

  //different options for CSV reader
  //https://spark.apache.org/docs/latest/sql-data-sources-csv.html

  val myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", StringType, true),
    new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    new StructField("count", LongType, false)
  ))
  val csvDF = spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST") //so fail on malformed data not matching our schema for one
    .schema(myManualSchema)
    .load("src/resources/flight-data/csv/2010-summary.csv") //we have our DataFrame at this point

  csvDF.show(5)

  csvDF.describe().show()

  //Things get tricky when we don’t expect our data to be in a certain format, but it comes in that
  //way, anyhow.
  //In general, Spark will fail only at job execution time rather than DataFrame definition time—
  //even if, for example, we point to a file that does not exist. This is due to lazy evaluation

  //For instance, we can take our CSV file and write it out as a TSV file quite easily:
  csvDF.write
    .format("csv")
    .mode("overwrite")
    .option("sep", "\t")
    .option("header", true) //we want to preserve headers
    .save("src/resources/tmp/my-tsv-file.tsv") //so Spark will create needed folders if they do not exist


  //JSON Files
  //Those coming from the world of JavaScript are likely familiar with JavaScript Object Notation,
  //or JSON, as it’s commonly called. There are some catches when working with this kind of data
  //that are worth considering before we jump in. In Spark, when we refer to JSON files, we refer to
  //line-delimited JSON files. This contrasts with files that have a large JSON object or array per
  //file.
  //The line-delimited versus multiline trade-off is controlled by a single option: multiLine. When
  //you set this option to true, you can read an entire file as one json object and Spark will go
  //through the work of parsing that into a DataFrame. Line-delimited JSON is actually a much more
  //stable format because it allows you to append to a file with a new record (rather than having to
  //read in an entire file and then write it out), which is what we recommend that you use. Another
  //key reason for the popularity of line-delimited JSON is because JSON objects have structure,
  //and JavaScript (on which JSON is based) has at least basic types. This makes it easier to work
  //with because Spark can make more assumptions on our behalf about the data. You’ll notice that
  //there are significantly less options than we saw for CSV because of the objects

  //https://spark.apache.org/docs/latest/sql-data-sources-json.html

  val jsonDF = spark.read
    .format("json")
    .option("mode", "FAILFAST") //so fail on error instead of making null values
    .schema(myManualSchema) //optional
      .load("src/resources/flight-data/json/2010-summary.json")

    jsonDF.describe().show()
    jsonDF.show(5)

  //Writing JSON Files
  //Writing JSON files is just as simple as reading them, and, as you might expect, the data source
  //does not matter. Therefore, we can reuse the CSV DataFrame that we created earlier to be the
  //source for our JSON file. This, too, follows the rules that we specified before: one file per
  //partition will be written out, and the entire DataFrame will be written out as a folder. It will also
  //have one JSON object per line:

  csvDF.orderBy(desc("count"))
    .limit(10) //so just top 10 rows of our current DataFramm ordered descending by count
    .write
    .format("json")
    .mode("overwrite")
    .save("src/resources/tmp/my-json-file.json")
}
