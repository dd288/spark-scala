package sample.Main


object Question2 {
  def main(args: Array[String]) {
    import org.apache.log4j.{Level, Logger} //Remove minor info and error messages
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Import required libraries
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("AirlineSentimentAnalysis")
      .master("local[*]")
      .getOrCreate()

    // Read the CSV file and create a DataFrame
    val df = spark.read.option("header", "true").csv("/home/giannispierr/Desktop/Σχολη 2022/Πλη47/Ergasia4/A4Q2Test/input/tweets.csv")

    // Remove every punctuation and convert every character to lower case
    val cleanText = udf((text: String) => text.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase())
    val cleanedDf = df.withColumn("clean_text", cleanText(col("text")))

    //testing
    val sentiment = when(col("negativereason_confidence") > 0.5, col("airline_sentiment")).otherwise(lit("neutral"))
    val sentimentDf = cleanedDf.withColumn("sentiment", sentiment)
    /**
     * This code filters out negative airline tweets from the sentimentDf DataFrame and then groups them by airline and negative reason,
     * counts the occurrences of each group, orders them by airline and count,
     * groups them again by airline, and finally aggregates the main negative reason for each airline in a new DataFrame called mainComplaints.
     */
    val negativeDf = sentimentDf.filter(col("airline_sentiment") === "negative")
    val mainComplaints = negativeDf.groupBy("airline", "negativereason")
      .count()
      .orderBy(col("airline"), desc("count"))
      .groupBy("airline")
      .agg(first("negativereason").alias("main_complaint"))



    // Filter out tweets with negativereason_confidence <= 0.5
    //val filteredDf = cleanedDf.filter(col("negativereason_confidence") > 0.5)
    //val filteredDf = cleanedDf.filter(col("negativereason_confidence") > 0.5 || col("airline_sentiment").isin("positive", "neutral"))
    // Filter out neutral and positive tweets
    //val filteredDf = cleanedDf.filter(!col("airline_sentiment").isin("neutral", "positive"))

    // Task 1: Find the 5 most used words in a tweet for each airline_sentiment
    val tokenizer = new org.apache.spark.ml.feature.Tokenizer().setInputCol("clean_text").setOutputCol("words")
    val wordsDf = tokenizer.transform(sentimentDf)

    //wordsDf.select("airline_sentiment").distinct().show()


    // Define a function to count the most frequent words
    def getMostFrequentWords(df: org.apache.spark.sql.DataFrame, sentiment: String, topN: Int): Unit = {
      val sentimentDf = df.filter(col("airline_sentiment") === sentiment)
      val wordCount = sentimentDf.select(explode(col("words")).alias("word")).groupBy("word").count().sort(desc("count")).limit(topN)
      println(s"Top $topN most used words for $sentiment sentiment:")
      wordCount.show()
    }

    // Call the function for each sentiment
    getMostFrequentWords(wordsDf, "positive", 5)
    getMostFrequentWords(wordsDf, "negative", 5)
    getMostFrequentWords(wordsDf, "neutral", 5)

    // Task 2: Find the main form of complaints for each airline company
    //val mainComplaints = sentimentDf.groupBy("airline", "negativereason").count().orderBy(col("airline"), desc("count")).groupBy("airline").agg(first("negativereason").alias("main_complaint"))
    println("Main form of complaints for each airline company:")
    mainComplaints.show()
  }
}

