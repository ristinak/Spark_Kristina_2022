package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.ml.recommendation.ALS

object Day37RecommendationEngine extends App {
  println("Ch28: Recommendations")
  val spark = getSpark("Sparky")

  //Chapter 28. Recommendation
  //The task of recommendation is one of the most intuitive. By studying people’s explicit
  //preferences (through ratings) or implicit preferences (through observed behavior), you can make
  //recommendations on what one user may like by drawing similarities between the user and other
  //users, or between the products they liked and other products. Using the underlying similarities,
  //recommendation engines can make new recommendations to other users.
  //Use Cases
  //Recommendation engines are one of the best use cases for big data. It’s fairly easy to collect
  //training data about users’ past preferences at scale, and this data can be used in many domains to
  //connect users with new content. Spark is an open source tool of choice used across a variety of
  //companies for large-scale recommendations:
  //Movie recommendations
  //Amazon, Netflix, and HBO all want to provide relevant film and TV content to their users.
  //Netflix utilizes Spark, to make large scale movie recommendations to their users.
  //Course recommendations
  //A school might want to recommend courses to students by studying what courses similar
  //students have liked or taken. Past enrollment data makes for a very easy to collect training
  //dataset for this task.
  //In Spark, there is one workhorse recommendation algorithm, Alternating Least Squares (ALS).
  //This algorithm leverages a technique called collaborative filtering, which makes
  //recommendations based only on which items users interacted with in the past. That is, it does not
  //require or use any additional features about the users or the items. It supports several ALS
  //variants (e.g., explicit or implicit feedback). Apart from ALS, Spark provides Frequent Pattern
  //Mining for finding association rules in market basket analysis. Finally, Spark’s RDD API also
  //includes a lower-level matrix factorization method that will not be covered in this book.
  //Collaborative Filtering with Alternating Least Squares
  //ALS finds a �-dimensional feature vector for each user and item such that the dot product of
  //each user’s feature vector with each item’s feature vector approximates the user’s rating for that
  //item. Therefore this only requires an input dataset of existing ratings between user-item pairs,
  //with three columns: a user ID column, an item ID column (e.g., a movie), and a rating column.
  //The ratings can either be explicit—a numerical rating that we aim to predict directly—or implicit
  //—in which case each rating represents the strength of interactions observed between a user and
  //item (e.g., number of visits to a particular page), which measures our level of confidence in the
  //user’s preference for that item. Given this input DataFrame, the model will produce feature
  //vectors that you can use to predict users’ ratings for items they have not yet rated.
  //One issue to note in practice is that this algorithm does have a preference for serving things that
  //are very common or that it has a lot of information on. If you’re introducing a new product that
  //no users have expressed a preference for, the algorithm isn’t going to recommend it to many
  //people. Additionally, if new users are onboarding onto the platform, they may not have any
  //ratings in the training set. Therefore, the algorithm won’t know what to recommend them. These
  //are examples of what we call the cold start problem, which we discuss later on in the chapter.
  //In terms of scalability, one reason for Spark’s popularity for this task is that the algorithm and
  //implementation in MLlib can scale to millions of users, millions of items, and billions of ratings

  //This example will make use of a dataset that we have not used thus far in the book, the
  //MovieLens movie rating dataset. This dataset, naturally, has information relevant for making
  //movie recommendations. We will first use this dataset to train a mode

  val ratings = spark.read.textFile("src/resources/text/sample_movielens_ratings.txt")
    .selectExpr("split(value , '::') as col")
    .selectExpr(
      "cast(col[0] as int) as userId",
      "cast(col[1] as int) as movieId",
      "cast(col[2] as float) as rating",
      "cast(col[3] as long) as timestamp")
  val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

  training.show(10, false)
  training.describe().show(false)

  //we have a lot of parameters we can adjust
  val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.01) //default was 0.1 used to avoid overfitting
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating") //in real life you would want to multiple or add (maybe scaled) features into a single score
  println(als.explainParams())

  val alsModel = als.fit(training)
  val predictions = alsModel.transform(test)

  //lets check our recommendations
  predictions.show(10, false)

  //We can now output the top � recommendations for each user or movie. The model’s
  //recommendForAllUsers method returns a DataFrame of a userId, an array of
  //recommendations, as well as a rating for each of those movies. recommendForAllItems returns
  //a DataFrame of a movieId, as well as the top users for that movie:

  // in Scala
  alsModel.recommendForAllUsers(5).show(false) //so 5 recommendations for all users

  alsModel.recommendForAllItems(5).show(false) //so top 5 users who liked each movie

  //with explode we would show each recommendation in new row
  // in Scala
  alsModel.recommendForAllUsers(3)
    .selectExpr("userId", "explode(recommendations)").show()


  //Evaluators for Recommendation
  //When covering the cold-start strategy, we can set up an automatic model evaluator when
  //working with ALS. One thing that may not be immediately obvious is that this recommendation
  //problem is really just a kind of regression problem. Since we’re predicting values (ratings) for
  //given users, we want to optimize for reducing the total difference between our users’ ratings and
  //the true values. We can do this using the same RegressionEvaluator that we saw in
  //Chapter 27. You can place this in a pipeline to automate the training process. When doing this,
  //you should also set the cold-start strategy to be drop instead of NaN and then switch it back to
  //NaN when it comes time to actually make predictions in your production system

  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")
  val rmse = evaluator.evaluate(predictions)
  println(s"Root-mean-square error = $rmse")
  //so we would want to play with hyperparameters and try to minimize this RMSE

  //Frequent Pattern Mining
  //In addition to ALS, another tool that MLlib provides for creating recommendations is frequent
  //pattern mining. Frequent pattern mining, sometimes referred to as market basket analysis, looks
  //at raw data and finds association rules. For instance, given a large number of transactions it
  //might identify that users who buy hot dogs almost always purchase hot dog buns. This technique
  //can be applied in the recommendation context, especially when people are filling shopping carts
  //(either on or offline). Spark implements the FP-growth algorithm for frequent pattern mining //to convert Seq TDF
  import spark.implicits._
  val dataset = spark.createDataset(Seq(
    "milk cookies chocolate",
    "milk cookies champagne chocolate",
    "milk cookies",
    "milk")
  ).map(t => t.split(" ")).toDF("items")

  dataset.show()
  dataset.printSchema() //so items should be Array type

  val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.6)
  val model = fpgrowth.fit(dataset)

  // Display frequent itemsets.
  model.freqItemsets.show()

  // Display generated association rules.
  model.associationRules.show()

  // transform examines the input items against all the association rules and summarize the
  // consequents as prediction
  model.transform(dataset).show()

  //TODO check how large a dataset it can mine, presumably since it is in Spark it is quite big

}
