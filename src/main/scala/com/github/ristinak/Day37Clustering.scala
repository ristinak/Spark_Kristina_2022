package com.github.ristinak

import breeze.linalg.max
import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{RFormula, VectorAssembler}
import org.apache.spark.sql.DataFrame

object Day37Clustering extends App {
  val spark = getSpark("Sparky")

  val filePath = "src/resources/irises/iris.data"

  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)
    .toDF("petal_width","petal_height", "sepal_width", "sepal_height", "irisType")
  //so I could rename immediately instead of using withColumnRenamed below


//  val flowerDF = df.withColumnRenamed("_c4", "irisType")
//  flowerDF.show()
//
//  flowerDF.describe().show() //check for any inconsistencies, maybe there are some outliers
  //maybe some missing data


  val myRFormula = new RFormula().setFormula("irisType ~ . ")
  //of course we could have use VectorAssembler instead since we do not need category transformed to numeric

  val fittedRF = myRFormula.fit(df)

  val preparedDF = fittedRF.transform(df)
  preparedDF.sample(0.2).show(false)

  //we will not divide in train and test sets here since we assume we have NO categories at all

  val km = new KMeans().setK(3) //we are sort of cheating since we know there are 3 types of irises!
    .setFeaturesCol("features") //this is actually default
    .setPredictionCol("prediction") //also default

  println(km.explainParams()) //there are quite a few to adjust

  val kmModel = km.fit(preparedDF)

  val summary = kmModel.summary
  println("Cluster sizes")
  summary.clusterSizes.foreach(println)

  println("Cluster Centers")
  kmModel.clusterCenters.foreach(println)

  val predictedDF = kmModel.transform(preparedDF)

  predictedDF.sample(0.2).show(false)

  // Evaluate clustering by computing Silhouette score
  val evaluator = new ClusteringEvaluator()
//uses Euclidian distance for evaluation by default
  //https://en.wikipedia.org/wiki/Euclidean_distance

  val silhouette = evaluator.evaluate(predictedDF)
  println(s"Silhouette with squared euclidean distance = $silhouette")

  //let's make a little testing function

  def testKMeans(df: DataFrame, k: Int): Double = {
    println(s"Testing Kmeans with $k clusters")
    val km = new KMeans().setK(k) //using features col as default for input

    val kmModel = km.fit(df)
    println("Cluster sizes")
    println(kmModel.summary.clusterSizes.mkString(","))

    val predictions = kmModel.transform(df)
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    silhouette
  }

  //so we check our silhoutte score from 2 to 8 divisions,
  //our best silhoutte score would be with 150 segments since we have 150 rows but that would be useless...
//  val silhouettes = (2 to 8).map(n => testKMeans(preparedDF, n))
//
//  println("Silhoutte scores from 2 to 8 K segments")
//  println(silhouettes.mkString(","))

  //TODO find optimal number of segments in the src/resources/csv/cluster_me.csv file
  //Use Silhouette calculations

  val newFilePath = "src/resources/csv/cluster_me.csv"

  val originalDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(newFilePath)
    .toDF("col1", "col2")

  val myVector = new VectorAssembler()
    .setInputCols(Array("col1", "col2"))
    .setOutputCol("features")

  val clusterDF = myVector.transform(originalDF)

  println()
  println("******* New clustering data *******")
  println()

  val minCluster = 2
  val maxCluster = 20
  val newSilhouettes = (minCluster to maxCluster).map(n => testKMeans(clusterDF, n))
  println("Silhouette scores from 2 to 20 K segments")
  println(newSilhouettes.mkString(","))

  val bestSilhouette = newSilhouettes.max
  val bestNumClusters = newSilhouettes.indexOf(bestSilhouette) + minCluster

  println()
  println(s"******* The best number of clusters is ${bestNumClusters} with silhouette $bestSilhouette *******")
  println()

  val kMean = new KMeans().setK(bestNumClusters)
  val clusteredDF = kMean.fit(clusterDF).transform(clusterDF)

  //show dataframe with these optimal segments
  println("Clustered DF:")
  clusteredDF.sample(0.5).orderBy("prediction").show(false)


}
