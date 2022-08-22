package com.github.ristinak

import com.github.ristinak.SparkUtil.getSpark
import org.apache.spark.ml.feature.{Bucketizer, CountVectorizer, MinMaxScaler, NGram, QuantileDiscretizer, RegexTokenizer, StopWordsRemover, StringIndexer, Tokenizer, VectorAssembler}

object Day34FeatureTransformation extends App {
  println("Ch24: Working with Continuous Features and Categorical")
  val spark = getSpark("Sparky")

  //Working with Continuous Features
  //Continuous features are just values on the number line, from positive infinity to negative infinity.
  //There are two common transformers for continuous features. First, you can convert continuous
  //features into categorical features via a process called bucketing, or you can scale and normalize
  //your features according to several different requirements. These transformers will only work on
  //Double types, so make sure you’ve turned any other numerical values to Double

  // in Scala
  val contDF = spark
    .range(20)
    .selectExpr("cast(id as double)")

  contDF.show()

  //To specify the bucket, set its borders. For example, setting splits to 5.0, 10.0, 250.0 on our
  //contDF will actually fail because we don’t cover all possible input ranges. When specifying your
  //bucket points, the values you pass into splits must satisfy three requirements:
  //The minimum value in your splits array must be less than the minimum value in your
  //DataFrame.
  //The maximum value in your splits array must be greater than the maximum value in
  //your DataFrame.
  //You need to specify at a minimum three values in the splits array, which creates two
  //buckets.

  //WARNING
  //The Bucketizer can be confusing because we specify bucket borders via the splits method, but these
  //are not actually splits.
  //To cover all possible ranges, scala.Double.NegativeInfinity might be another split option,
  //with scala.Double.PositiveInfinity to cover all possible ranges outside of the inner splits.
  //In Python we specify this in the following way: float("inf"), float("-inf").
  //In order to handle null or NaN values, we must specify the handleInvalid parameter as a
  //certain value. We can either keep those values (keep), error or null, or skip those rows.
  //Here’s an example of using bucketing:

  //so you might want to check min and max values beforehand (you could even do it programmatically

  val bucketBorders = Array(-1.0, 5.0, 10.0, 250.0, 600.0)
  val bucketer = new Bucketizer().setSplits(bucketBorders).setInputCol("id")
  bucketer.transform(contDF).show()

  //In addition to splitting based on hardcoded values, another option is to split based on percentiles
  //in our data. This is done with QuantileDiscretizer, which will bucket the values into user-
  //specified buckets with the splits being determined by approximate quantiles values. For instance,
  //the 90th quantile is the point in your data at which 90% of the data is below that value. You can
  //control how finely the buckets should be split by setting the relative error for the approximate
  //quantiles calculation using setRelativeError. Spark does this is by allowing you to specify the
  //number of buckets you would like out of the data and it will split up your data accordingly. The
  //following is an example

  val quantBucketer = new QuantileDiscretizer()
    .setNumBuckets(5)
    .setRelativeError(0.01) //play around with the relative error values
    .setInputCol("id")
  //unlike regular Bucketizer you first have to fit the data, because the model needs to learn the quantiles (the distribution)
  val fittedBucketer = quantBucketer.fit(contDF)
  fittedBucketer.transform(contDF).show() //of course you could do this in one line

  quantBucketer.fit(contDF).transform(contDF).show() //same as above two lines

  //Scaling and Normalization
  //We saw how we can use bucketing to create groups out of continuous variables. Another
  //common task is to scale and normalize continuous data. While not always necessary, doing so is
  //usually a best practice. You might want to do this when your data contains a number of columns
  //based on different scales. For instance, say we have a DataFrame with two columns: weight (in
  //ounces) and height (in feet). If you don’t scale or normalize, the algorithm will be less sensitive
  //to variations in height because height values in feet are much lower than weight values in
  //ounces. That’s an example where you should scale your data.
  //An example of normalization might involve transforming the data so that each point’s value is a
  //representation of its distance from the mean of that column. Using the same example from
  //before, we might want to know how far a given individual’s height is from the mean height.
  //Many algorithms assume that their input data is normalized.
  //As you might imagine, there are a multitude of algorithms we can apply to our data to scale or
  //normalize it. Enumerating them all is unnecessary here because they are covered in many other
  //texts and machine learning libraries.

  //StandardScaler
  //The StandardScaler standardizes a set of features to have zero mean and a standard deviation
  //of 1. The flag withStd will scale the data to unit standard deviation while the flag withMean
  //(false by default) will center the data prior to scaling it.
  //we did this in Day 33 Preprocessing Scala

  //MinMaxScaler
  //The MinMaxScaler will scale the values in a vector (component wise) to the proportional values
  //on a scale from a given min value to a max value. If you specify the minimum value to be 0 and
  //the maximum value to be 1, then all the values will fall in between 0 and 1

  // in Scala

  val minMax = new MinMaxScaler().setMin(5).setMax(12)
    .setInputCol("features")
    .setOutputCol("minmaxScale")
  //so we "squeeze" our values in a range 5 to 10 but I need VectorAssembler first

  val va = new VectorAssembler()
    .setInputCols(Array("id")) //we only have a single column but we still need to put it in a vector
    .setOutputCol("features")
  val vectorDF = va.transform(contDF)

  vectorDF.show()


  minMax.fit(vectorDF).transform(vectorDF).show()

  val sales = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/resources/retail-data/by-day/*.csv")
    .coalesce(5)
    .where("Description IS NOT NULL")
  val fakeIntDF = spark.read.parquet("src/resources/simple-ml-integers")
  var simpleDF = spark.read.json("src/resources/simple-ml")
  val scaleDF = spark.read.parquet("src/resources/simple-ml-scaling")

  //Working with Categorical Features
  //The most common task for categorical features is indexing. Indexing converts a categorical
  //variable in a column to a numerical one that you can plug into machine learning algorithms.
  //While this is conceptually simple, there are some catches that are important to keep in mind so
  //that Spark can do this in a stable and repeatable manner.
  //In general, we recommend re-indexing every categorical variable when pre-processing just for
  //consistency’s sake. This can be helpful in maintaining your models over the long run as your
  //encoding practices may change over time.
  //StringIndexer
  //The simplest way to index is via the StringIndexer, which maps strings to different numerical
  //IDs. Spark’s StringIndexer also creates metadata attached to the DataFrame that specify what
  //inputs correspond to what outputs. This allows us later to get inputs back from their respective
  //index values:

  val lblIndxr = new StringIndexer()
//    .setInputCol("lab")
    .setInputCol("color")
    .setOutputCol("labelInd")
  val idxRes = lblIndxr.fit(simpleDF).transform(simpleDF)
  idxRes.show()

  //important often categories are better transformed into one hot encoding -

  //One-Hot Encoding
  //Indexing categorical variables is only half of the story. One-hot encoding is an extremely
  //common data transformation performed after indexing categorical variables. This is because
  //indexing does not always represent our categorical variables in the correct way for downstream
  //models to process. For instance, when we index our “color” column, you will notice that some
  //colors have a higher value (or index number) than others (in our case, blue is 1 and green is 2).
  //This is incorrect because it gives the mathematical appearance that the input to the machine
  //learning algorithm seems to specify that green > blue, which makes no sense in the case of the
  //current categories. To avoid this, we use OneHotEncoder, which will convert each distinct value
  //to a Boolean flag (1 or 0) as a component in a vector. When we encode the color value, then we
  //can see these are no longer ordered, making them easier for downstream models (e.g., a linear
  //model) to process:

  //Text Data Transformers
  //Text is always tricky input because it often requires lots of manipulation to map to a format that
  //a machine learning model will be able to use effectively. There are generally two kinds of texts
  //you’ll see: free-form text and string categorical variables. This section primarily focuses on free-
  //form text because we already discussed categorical variables.
  //Tokenizing Text
  //Tokenization is the process of converting free-form text into a list of “tokens” or individual
  //words. The easiest way to do this is by using the Tokenizer class. This transformer will take a
  //string of words, separated by whitespace, and convert them into an array of words. For example,
  //in our dataset we might want to convert the Description field into a list of tokens.

  val tkn = new Tokenizer().setInputCol("Description").setOutputCol("DescOut")
  val tokenized = tkn.transform(sales.select("Description"))
  tokenized.show(false)

  //We can also create a Tokenizer that is not just based white space but a regular expression with
  //the RegexTokenizer. The format of the regular expression should conform to the Java Regular
  //Expression (RegEx) syntax:

  val rt = new RegexTokenizer()
    .setInputCol("Description")
    .setOutputCol("DescOut")
    .setPattern(" ") // simplest expression so overkill here, if we are using regex we want something more complex
    .setToLowercase(true)
  rt.transform(sales.select("Description")).show(false)

  //Removing Common Words
  //A common task after tokenization is to filter stop words, common words that are not relevant in
  //many kinds of analysis and should thus be removed. Frequently occurring stop words in English
  //include “the,” “and,” and “but.” Spark contains a list of default stop words you can see by calling
  //the following method, which can be made case insensitive if necessary (as of Spark 2.2,
  //supported languages for stopwords are “danish,” “dutch,” “english,” “finnish,” “french,”
  //“german,” “hungarian,” “italian,” “norwegian,” “portuguese,” “russian,” “spanish,” “swedish,”
  //and “turkish”):

  val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
  //https://spark.apache.org/docs/3.2.0/api/java/org/apache/spark/ml/feature/StopWordsRemover.html#loadDefaultStopWords-java.lang.String-
  //since the result is just an array of strings we could load our own from file
  //for Latvian you could download txt file from here
  //https://github.com/stopwords-iso/stopwords-lv/blob/master/stopwords-lv.txt
  //and split into an Array of Strings
  //stopwords you want to curate yourself, depending on use case

  val stops = new StopWordsRemover()
    .setStopWords(englishStopWords) //what we are going to remove
    .setInputCol("DescOut")
    .setOutputCol("DescOutNoStopWords")
  stops.transform(tokenized).show(false)

  //Creating Word Combinations
  //Tokenizing our strings and filtering stop words leaves us with a clean set of words to use as
  //features. It is often of interest to look at combinations of words, usually by looking at colocated
  //words. Word combinations are technically referred to as n-grams—that is, sequences of words of
  //length n. An n-gram of length 1 is called a unigrams; those of length 2 are called bigrams, and
  //those of length 3 are called trigrams (anything above those are just four-gram, five-gram, etc.),
  //Order matters with n-gram creation, so converting a sentence with three words into bigram
  //representation would result in two bigrams. The goal when creating n-grams is to better capture
  //sentence structure and more information than can be gleaned by simply looking at all words
  //individually. Let’s create some n-grams to illustrate this concept.
  //The bigrams of “Big Data Processing Made Simple” are:
  //“Big Data”
  //“Data Processing”
  //“Processing Made”
  //“Made Simple”
  //While the trigrams are:
  //“Big Data Processing”
  //“Data Processing Made”
  //“Procesing Made Simple”
  //With n-grams, we can look at sequences of words that commonly co-occur and use them as
  //inputs to a machine learning algorithm. These can create better features than simply looking at
  //all of the words individually (say, tokenized on a space character):

  val unigram = new NGram().setInputCol("DescOut").setN(1)
  val bigram = new NGram().setInputCol("DescOut").setN(2)
  //so trigrams would be setN(3)
  unigram.transform(tokenized.select("DescOut")).show(false)
  bigram.transform(tokenized.select("DescOut")).show(false)

  //Converting Words into Numerical Representations
  //Once you have word features, it’s time to start counting instances of words and word
  //combinations for use in our models. The simplest way is just to include binary counts of a word
  //in a given document (in our case, a row). Essentially, we’re measuring whether or not each row
  //contains a given word. This is a simple way to normalize for document sizes and occurrence
  //counts and get numerical features that allow us to classify documents based on content. In
  //addition, we can count words using a CountVectorizer, or reweigh them according to the
  //prevalence of a given word in all the documents using a TF–IDF transformation (discussed next).
  //A CountVectorizer operates on our tokenized data and does two things:
  //1. During the fit process, it finds the set of words in all the documents and then counts
  //the occurrences of those words in those documents.
  //2. It then counts the occurrences of a given word in each row of the DataFrame column
  //during the transformation process and outputs a vector with the terms that occur in that
  //row.
  //Conceptually this tranformer treats every row as a document and every word as a term and the
  //total collection of all terms as the vocabulary. These are all tunable parameters, meaning we can
  //set the minimum term frequency (minTF) for the term to be included in the vocabulary
  //(effectively removing rare words from the vocabulary); minimum number of documents a term
  //must appear in (minDF) before being included in the vocabulary (another way to remove rare
  //words from the vocabulary); and finally, the total maximum vocabulary size (vocabSize).
  //Lastly, by default the CountVectorizer will output the counts of a term in a document. To just
  //return whether or not a word exists in a document, we can use setBinary(true). Here’s an
  //example of using CountVectorizer:

  val cv = new CountVectorizer()
    .setInputCol("DescOut")
    .setOutputCol("countVec")
    .setVocabSize(500)
    .setMinTF(1) //so term has to appear at least once
    .setMinDF(2) //and this term has to appear in at least two documents //so single use somewhere will not be shown
  val fittedCV = cv.fit(tokenized)

  fittedCV.transform(tokenized).show(false)

  //we can print out vocabulary , so here I print words index 30 to 49
  println(fittedCV.vocabulary.slice(30,50).mkString(","))

  //Term frequency–inverse document frequency
  //Another way to approach the problem of converting text into a numerical representation is to use
  //term frequency–inverse document frequency (TF–IDF). In simplest terms, TF–IDF measures
  //how often a word occurs in each document, weighted according to how many documents that
  //word occurs in. The result is that words that occur in a few documents are given more weight
  //than words that occur in many documents. In practice, a word like “the” would be weighted very
  //low because of its prevalence while a more specialized word like “streaming” would occur in
  //fewer documents and thus would be weighted higher. In a way, TF–IDF helps find documents
  //that share similar topics. Let’s take a look at an example—first, we’ll inspect some of the
  //documents in our data containing the word “red”

  // in Scala
  val tfIdfIn = tokenized
    .where("array_contains(DescOut, 'red')")
    .select("DescOut")
    .limit(10)
  tfIdfIn.show(false)

//  /We can see some overlapping words in these documents, but these words provide at least a rough
  //topic-like representation. Now let’s input that into TF–IDF. To do this, we’re going to hash each
  //word and convert it to a numerical representation, and then weigh each word in the voculary
  //according to the inverse document frequency. Hashing is a similar process as CountVectorizer,
  //but is irreversible—that is, from our output index for a word, we cannot get our input word
  //(multiple words might map to the same output index)

  // in Scala
  import org.apache.spark.ml.feature.{HashingTF, IDF}
  val tf = new HashingTF()
    .setInputCol("DescOut")
    .setOutputCol("TFOut")
    .setNumFeatures(10000)
  val idf = new IDF()
    .setInputCol("TFOut")
    .setOutputCol("IDFOut")
    .setMinDocFreq(2)

  // in Scala
  idf.fit(tf.transform(tfIdfIn)).transform(tf.transform(tfIdfIn)).show(false)

  //While the output is too large to include here, notice that a certain value is assigned to “red” and
  //that this value appears in every document. Also note that this term is weighted extremely low
  //because it appears in every document. The output format is a sparse Vector we can subsequently
  //input into a machine learning model in a form like this:
  //(10000,[2591,4291,4456],[1.0116009116784799,0.0,0.0])


  //TODO using tokenized alice - from weekend exercise
  //TODO remove english stopwords

  //Create a CountVectorized of words/tokens/terms that occur in at least 3 documents(here that means rows)
  //the terms have to occur at least 1 time in each row

  //TODO show first 30 rows of data


}
