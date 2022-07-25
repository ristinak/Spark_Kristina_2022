package com.github.ristinak

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format}

object Day19KMeansExample extends App {
  println(s"Testing KMeans Clustering with Scala : ${util.Properties.versionNumberString}")
  //https://en.wikipedia.org/wiki/K-means_clustering
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  val staticDataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true") //we are letting Spark figure out the type of data in our csvs
    //        .csv("src/resources/retail-data/by-day/2010-12-01.csv")
    .csv("src/resources/retail-data/by-day/2010-12-*.csv") //so all of december 2010
  //    .csv("src/resources/retail-data/by-day/*.csv") //in order for this to work Hadoop (via Wintils on Windows) should be installed
  //get Hadoop with Winutils for Windows from https://github.com/kontext-tech/winutils
  //src/resources/retail-data/by-day
  //    .load("src/resources/retail-data/by-day/*.csv") //notice the wildcard we are loading everything!
  staticDataFrame.createOrReplaceTempView("retail_data")
  val staticSchema = staticDataFrame.schema
  println(staticSchema.toArray.mkString("\n"))

  println(s"We got ${staticDataFrame.count()} rows of data!")

  //Machine learning algorithms in MLlib require that data is represented as numerical values. Our
  //current data is represented by a variety of different types, including timestamps, integers, and
  //strings. Therefore we need to transform this data into some numerical representation. In this
  //instance, we’ll use several DataFrame transformations to manipulate our date data:

  //in machine learning a lot of time is spent doing data engineering
  //meaning we create new data out of existing data
  val preppedDataFrame = staticDataFrame
    .na.fill(0) //we are filling 0s where no values exist because ML algorithms hate empty values
    .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))
    .coalesce(5)


  //We are also going to need to split the data into training and test sets. In this instance, we are
  //going to do this manually by the date on which a certain purchase occurred; however, we could
  //also use MLlib’s transformation APIs to create a training and test set via train validation splits or
  //cross validation (these topics are covered at length in Part VI)
  val trainDataFrame = preppedDataFrame
    //    .where("InvoiceDate < '2011-07-01'")
    .where("InvoiceDate < '2010-12-18'")
  //    .where("InvoiceNo < '536572'") //quick and dirty split on string column ideally you want random split
  val testDataFrame = preppedDataFrame
    //    .where("InvoiceDate >= '2011-07-01'")
    .where("InvoiceDate >= '2010-12-18'") //of course last minute Xmas buys are not random
  //    .where("InvoiceNo >='536572'")

  //splits are usually 80% training and 20% testing

  println(s"Training set is ${trainDataFrame.count()} rows")
  println(s"Test set is ${testDataFrame.count()} rows")

  //Spark’s MLlib also provides a number of transformations with which we can automate
  //some of our general transformations. One such transformer is a StringIndexer:
  //again ML algorithms like numeric values!!
  val indexer = new StringIndexer()
    .setInputCol("day_of_week")
    .setOutputCol("day_of_week_index")

  //This will turn our days of weeks into corresponding numerical values. For example, Spark might
  //represent Saturday as 6, and Monday as 1. However, with this numbering scheme, we are
  //implicitly stating that Saturday is greater than Monday (by pure numerical values). This is
  //obviously incorrect. To fix this, we therefore need to use a OneHotEncoder to encode each of
  //these values as their own column. These Boolean flags state whether that day of week is the
  //relevant day of the week:

  val encoder = new OneHotEncoder()
    .setInputCol("day_of_week_index")
    .setOutputCol("day_of_week_encoded") //so each day will be representaed by some number flags(booleans true/false)

  //Each of these will result in a set of columns that we will “assemble” into a vector. All machine
  //learning algorithms in Spark take as input a Vector type, which must be a set of numerical
  //values:
  val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
    .setOutputCol("features")
  //  /https://en.wikipedia.org/wiki/One-hot


  //Here, we have three key features: the price, the quantity, and the day of week. Next, we’ll set this
  //up into a pipeline so that any future data we need to transform can go through the exact same
  //process:
  val transformationPipeline = new Pipeline()
    .setStages(Array(indexer, encoder, vectorAssembler))

  trainDataFrame.show(5) //well for one day our weekeday encoding is not going to be very useful

  //Preparing for training is a two-step process. We first need to fit our transformers to this dataset.
  //We cover this in depth in Part VI, but basically our StringIndexer needs to know how many
  //unique values there are to be indexed. After those exist, encoding is easy but Spark must look at
  //all the distinct values in the column to be indexed in order to store those values later on:

  // in Scala so we first do a sort of test run on our pipeline
  val fittedPipeline = transformationPipeline.fit(trainDataFrame)

  //After we fit the training data, we are ready to take that fitted pipeline and use it to transform all
  //of our data in a consistent and repeatable way
  val transformedTraining = fittedPipeline.transform(trainDataFrame)

  //let's see what we got out of this
  transformedTraining.show(10)

  //At this point, it’s worth mentioning that we could have included our model training in our
  //pipeline. We chose not to in order to demonstrate a use case for caching the data. Instead, we’re
  //going to perform some hyperparameter tuning on the model because we do not want to repeat the
  //exact same transformations over and over again; specifically, we’ll use caching, an optimization
  //that we discuss in more detail in Part IV. This will put a copy of the intermediately transformed
  //dataset into memory, allowing us to repeatedly access it at much lower cost than running the
  //entire pipeline again. If you’re curious to see how much of a difference this makes, skip this line
  //and run the training without caching the data. Then try it after caching; you’ll see the results are
  //significant

  transformedTraining.cache() //this turnson caching(saving the working data set in memory)

  //We now have a training set; it’s time to train the model. First we’ll import the relevant model
  //that we’d like to use and instantiate it:

  val kmeans = new KMeans()
    .setK(20) //so we are giving configuration for this kmeans model to create 20 clusters
    //the above number is so called hyperparameter and you would want to explore different values
    //when you do not know how many clusters there could be
    //theoretically you could have up to n clusters when you have n rows of data/features, but that would be useless
    .setSeed(1L)

  //In Spark, training machine learning models is a two-phase process. First, we initialize an
  //untrained model, and then we train it. There are always two types for every algorithm in MLlib’s
  //DataFrame API. They follow the naming pattern of Algorithm, for the untrained version, and
  //AlgorithmModel for the trained version. In our example, this is KMeans and then KMeansModel.
  //Estimators in MLlib’s DataFrame API share roughly the same interface that we saw earlier with
  //our preprocessing transformers like the StringIndexer. This should come as no surprise
  //because it makes training an entire pipeline (which includes the model) simple. For our purposes
  //here, we want to do things a bit more step by step, so we chose to not do this in this example:

  // in Scala
  val kmModel = kmeans.fit(transformedTraining) //modelling work happens here

  //here we apply our trained model we just did
  //to the our test data set which we set aside for well testing purposes
  val transformedTest = fittedPipeline.transform(testDataFrame)

  println("Our kmeans test results")
  transformedTest.show()

  println("Saving results to file")
  transformedTest.coalesce(1) //we want to move to single partition
    //we select a few columns //could have saved all without select
    //    .select("Quantity","day_of_week", "day_of_week_index", "InvoiceNo", "features") //features are compound data type with holdings
    .select("Quantity","day_of_week", "day_of_week_index", "InvoiceNo")
    .write
    .option("header", "true")
    .option("sep", ",")
    .mode("overwrite") //we will overwrite any existing
    .csv("src/resources/csv/kmeans-results.csv")
}

