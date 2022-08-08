package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{col, from_json, get_json_object, json_tuple, to_json}
import org.apache.spark.sql.types.{StructField, StructType, StringType}

object Day26JSONData extends App {
  println("CH6: Working with JSON")

  //https://www.json.org/json-en.html

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2011-08-04.csv"

  val df = readDataWithView(spark, filePath)

//  Spark has some unique support for working with JSON data. You can operate directly on strings
//    of JSON in Spark and parse from JSON or extract JSON objects. Let’s begin by creating a JSON
//    column:

  // in Scala
  val jsonDF = spark.range(1).selectExpr("""
'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""") //so triple quotes outside
  //single quotes to designate the string and double quotes for the actual JSON content

  jsonDF.printSchema()
  jsonDF.show(false)


  //You can use the get_json_object to inline query a JSON object, be it a dictionary or array.
  //You can use json_tuple if this object has only one level of nesting


  jsonDF.select(
    //so we went 3 levels deep to extract that 2 wegot key myJSONkey which brings us another object
    //{"myJSONValue":[1,2,3]}
    //then we get the value of that
    //finally we have array access of 2nd value (index 1)
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "jscol",
    json_tuple(col("jsonString"), "myJSONKey") as "jtupcol")
    .show(2, false)


  //we can go the other way
  //You can also turn a StructType into a JSON string by using the to_json function:

  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")))
    .show(5, false)

  //again JSON is just a specially formatted string basically

  //This function also accepts a dictionary (map) of parameters that are the same as the JSON data
  //source. You can use the from_json function to parse this (or other JSON data) back in. This
  //naturally requires you to specify a schema, and optionally you can specify a map of options, as
  //well:

  //so we hand code the schema
  val parseSchema = new StructType(Array(
    StructField("InvoiceNo", StringType, true),
    StructField("Description", StringType, true)))

  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")).alias("newJSON"))
    .select(from_json(col("newJSON"), parseSchema), col("newJSON"))
    .show(5, false)

  val dfParsedJson =   df.selectExpr("*", "(InvoiceNo, Description) as myStruct")
    .withColumn("newJSON", to_json(col("myStruct")))
    .withColumn("fromJSON", from_json(col("newJSON"), parseSchema ))

  dfParsedJson.printSchema()
  dfParsedJson.show(5, false)


}
