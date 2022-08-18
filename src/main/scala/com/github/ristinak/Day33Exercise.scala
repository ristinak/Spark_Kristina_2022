package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.functions.expr

object Day33Exercise extends App {

  val spark = getSpark("Sparky")

  //TODO Read text from url
  //https://www.gutenberg.org/files/11/11-0.txt - Alice in Wonderland
  //https://stackoverflow.com/questions/44961433/process-csv-from-rest-api-into-spark
  //above link shows how to read csv file from url
  //you can adopt it to read a simple text file directly as well

  val url = "https://www.gutenberg.org/files/11/11-0.txt"
  val dst = "src/resources/Alice.txt"
  Util.getTextFromWebAndSave(url, dst)

  //alternative download and read from file locally
  //TODO create a DataFrame with a single column called text which contains above book line by line

  val df = readDataWithView(spark, dst, source = "text").withColumnRenamed("value", "text")
  df.show(10, false)

  //TODO create new column called words with will contain Tokenized words of text column

  val tkn = new Tokenizer().setInputCol("text").setOutputCol("words")
  val dfWithWords = tkn.transform(df.select("text"))
  dfWithWords.show(10, false)

  //TODO create column called textLen which will be a character count in text column
  //https://spark.apache.org/docs/2.3.0/api/sql/index.html#length can use or also length function from spark

  val dfWithCharCount = dfWithWords.withColumn("charCount", expr("char_length(text)"))
  dfWithCharCount.show(10, false)

  //TODO create column wordCount which will be a count of words in words column
  //can use count or length - words column will be Array type

  // didn't work
//  val dfWithWordCount = dfWithCharCount.withColumn("wordCount", expr("count(words)"))
//  dfWithWordCount.show(10)

  //TODO create Vector Assembler which will transform textLen and wordCount into a column called features
  //features column will have a Vector with two of those values

  //TODO create StandardScaler which will take features column and output column called scaledFeatures
  //it should be using mean and variance (so both true)

  //TODO create a dataframe with all these columns - save to alice.csv file with all columns


}
