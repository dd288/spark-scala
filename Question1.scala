import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex



object Question1 {
  def main(args: Array[String]) {
    //Show only critical errors on the logger
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Set appname and master
    val conf = new SparkConf().setAppName("Average Word Length").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Read text file and remove punctuation and convert to lowercase
    val fileRDD = sc.textFile("/home/giannispierr/Desktop/Σχολη 2022/Πλη47/Ergasia4/A4Test11/input/SherlockHolmes.txt")
    val regex = new Regex("[^a-zA-Z0-9 ]")
    val cleanRDD = fileRDD.map(line => regex.replaceAllIn(line.toLowerCase, ""))

    // Split words and calculate average word length starting from a specific character
    val charRDD = cleanRDD.flatMap(line => line.split(" "))
      //filter letters and map them
      .filter(word => word.matches("^[a-z].*"))
      .map(word => (word.charAt(0), (word.length, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues(total => total._1.toDouble / total._2.toDouble)
      .sortBy(_._2, false)

    // Print results
    charRDD.collect().foreach(println)

    sc.stop()
  }
}
