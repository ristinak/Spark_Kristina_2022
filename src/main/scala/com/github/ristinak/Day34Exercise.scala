package com.github.ristinak

import com.github.ristinak.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.ml.feature.{Bucketizer, CountVectorizer, MinMaxScaler, NGram, QuantileDiscretizer, RegexTokenizer, StopWordsRemover, StringIndexer, Tokenizer, VectorAssembler}

object Day34Exercise extends App {

  val spark = getSpark("Sparky")

  //TODO using tokenized alice - from weekend exercise

  val url = "src/resources/Alice.txt"
  val df = readDataWithView(spark, url, source = "text", header = false)
    .withColumnRenamed("value", "text")
  df.cache()

  val tkn = new Tokenizer().setInputCol("text").setOutputCol("words")
  val dfWithWords = tkn.transform(df.select("text"))

  //TODO remove english stopwords

  val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
  val stops = new StopWordsRemover()
    .setStopWords(englishStopWords) //what we are going to remove
    .setInputCol("words")
    .setOutputCol("noStopWords")
  stops.transform(dfWithWords).show(20, false)

  //Create a CountVectorized of words/tokens/terms that occur in at least 3 documents(here that means rows)
  //the terms have to occur at least 2 times in each row
  //TODO show first 30 rows of data

  val cv = new CountVectorizer()
    .setInputCol("words")
    .setOutputCol("countVec")
    .setVocabSize(500)
    .setMinTF(2) //so term has to appear at least once
    .setMinDF(3) //and this term has to appear in at least two documents //so single use somewhere will not be shown
  val fittedCV = cv.fit(dfWithWords)

  fittedCV.transform(dfWithWords).show(30, false)

  //we can print out vocabulary , so here I print words index 30 to 49
  println(fittedCV.vocabulary.mkString(","))

}
