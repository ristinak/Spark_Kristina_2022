package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg.Vectors
//WARNING
//Confusingly, there are similar datatypes that refer to ones that can be used in DataFrames and others
//that can only be used in RDDs. The RDD implementations fall under the mllib package while the
//DataFrame implementations fall under ml

object Day32MLintro extends App {

  val spark = getSpark("Sparky")

  //Low-level data types
  //In addition to the structural types for building pipelines, there are also several lower-level data
  //types you may need to work with in MLlib (Vector being the most common). Whenever we pass
  //a set of features into a machine learning model, we must do it as a vector that consists of
  //Doubles. This vector can be either sparse (where most of the elements are zero) or dense (where
  //there are many unique values). Vectors are created in different ways. To create a dense vector,
  //we can specify an array of all the values. To create a sparse vector, we can specify the total size
  //and the indices and values of the non-zero elements. Sparse is the best format, as you might have
  //guessed, when the majority of values are zero as this is a more compressed representation. Here
  //is an example of how to manually create a Vector:

  //not to be confuses with Scala Vector which is different (but actually has many similarities)
  val denseVec = Vectors.dense(1.0, 2.0, 3.0)
  println(denseVec)
  val arrDoubles = denseVec.toArray //we can convert to Array when we need to
  println(arrDoubles.mkString(","))

  //sparse Vector
  val size = 15
  val idx = Array(1,6,9) // locations of non-zero elements in vector
  val values = Array(2.0,3.0, 50.0)
  //useful for storing data when there are a lot of missing values
  val sparseVec = Vectors.sparse(size, idx, values)
  println(sparseVec)

  val arrDoublesAgain = sparseVec.toArray
  println(arrDoublesAgain.mkString(","))

  // in Scala
  var df = spark.read.json("src/resources/simple-ml")
  df.orderBy("value2").show()

  //This dataset consists of a categorical label with two values (good or bad), a categorical variable
  //(color), and two numerical variables. While the data is synthetic, let’s imagine that this dataset
  //represents a company’s customer health. The “color” column represents some categorical health
  //rating made by a customer service representative. The “lab” column represents the true customer
  //health. The other two values are some numerical measures of activity within an application (e.g.,
  //minutes spent on site and purchases). Suppose that we want to train a classification model where
  //we hope to predict a binary variable—the label—from the other values.

  //Feature Engineering with Transformers
  //As already mentioned, transformers help us manipulate our current columns in one way or
  //another. Manipulating these columns is often in pursuit of building features (that we will input
  //into our model). Transformers exist to either cut down the number of features, add more features,
  //manipulate current ones, or simply to help us format our data correctly. Transformers add new
  //columns to DataFrames.
  //When we use MLlib, all inputs to machine learning algorithms (with several exceptions
  //discussed in later chapters) in Spark must consist of type Double (for labels) and
  //Vector[Double] (for features). The current dataset does not meet that requirement and therefore
  //we need to transform it to the proper format.
  //To achieve this in our example, we are going to specify an RFormula. This is a declarative
  //language for specifying machine learning transformations and is simple to use once you
  //understand the syntax. RFormula supports a limited subset of the R operators that in practice
  //work quite well for simple models and manipulations (we demonstrate the manual approach to
  //this problem in Chapter 25). The basic RFormula operators are

  //~
  //Separate target and terms
  //+
  //Concat terms; “+ 0” means removing the intercept (this means that the y-intercept of the line
  //that we will fit will be 0)
  //-
  //Remove a term; “- 1” means removing the intercept (this means that the y-intercept of the
  //line that we will fit will be 0—yes, this does the same thing as “+ 0”
  //:
  //Interaction (multiplication for numeric values, or binarized categorical values)
  //.
  //All columns except the target/dependent variable
  //In order to specify transformations with this syntax, we need to import the relevant class. Then
  //we go through the process of defining our formula. In this case we want to use all available
  //variables (the .) and also add in the interactions between value1 and color and value2 and
  //color, treating those as new features:

  val supervised = new RFormula()
    .setFormula("lab ~ . + color:value1 + color:value2")

//  At this point, we have declaratively specified how we would like to change our data into what we
//  will train our model on. The next step is to fit the RFormula transformer to the data to let it
//  discover the possible values of each column. Not all transformers have this requirement but
//  because RFormula will automatically handle categorical variables for us, it needs to determine
//    which columns are categorical and which are not, as well as what the distinct values of the
//  categorical columns are. For this reason, we have to call the fit method. Once we call fit, it
//  returns a “trained” version of our transformer we can then use to actually transform our data

  val fittedRF = supervised.fit(df) //in this step RFormula "learns" how to transform data

  val preparedDF = fittedRF.transform(df)
  preparedDF.show(false)

  //n the output we can see the result of our transformation—a column called features that has our
  //previously raw data. What’s happening behind the scenes is actually pretty simple. RFormula
  //inspects our data during the fit call and outputs an object that will transform our data according
  //to the specified formula, which is called an RFormulaModel. This “trained” transformer always
  //has the word Model in the type signature. When we use this transformer, Spark automatically
  //converts our categorical variable to Doubles so that we can input it into a (yet to be specified)
  //machine learning model. In particular, it assigns a numerical value to each possible color
  //category, creates additional features for the interaction variables between colors and
  //value1/value2, and puts them all into a single vector. We then call transform on that object in
  //order to transform our input data into the expected output data.
  //Thus far you (pre)processed the data and added some features along the way. Now it is time to
  //actually train a model (or a set of models) on this dataset. In order to do this, you first need to
  //prepare a test set for evaluation.
  //TIP
  //Having a good test set is probably the most important thing you can do to ensure you train a model you
  //can actually use in the real world (in a dependable way). Not creating a representative test set or using
  //your test set for hyperparameter tuning are surefire ways to create a model that does not perform well
  //in real-world scenarios. Don’t skip creating a test set—it’s a requirement to know how well your
  //model actually does!
  //Let’s create a simple test set based off a random split of the data now (we’ll be using this test set
  //throughout the remainder of the chapter):

  //so we want to split our data into a training and test set

  val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3)) //so 70% to 30%
  //common is 70 to 90% training set and then 30-10% test set
  //again idea is that the sets are separate! - less overfitting!

  train.describe().show()

  test.describe().show()


}
