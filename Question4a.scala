import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// Define a case class for our edges
case class Edge(from: Int, to: Int)

object Question4a {
  def main(args: Array[String]) {

    import org.apache.log4j.{Level, Logger} //Remove minor info and error messages
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("Graph Analysis").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Load the edge list file into an RDD, skipping comments
    val edges = sc.textFile("/home/giannispierr/Desktop/Σχολη 2022/Πλη47/Ergasia4/a4q4atest/src/input/web-Stanford.txt")
      .filter(!_.startsWith("#"))
      .map(line => {
        val Array(from, to) = line.split("\t").map(_.toInt)
        Edge(from, to)
      })

    // Compute the number of incoming and outgoing edges for each node
    val incomingEdges = edges.map(edge => (edge.to, 1))
      .reduceByKey(_ + _)
    val outgoingEdges = edges.map(edge => (edge.from, 1))
      .reduceByKey(_ + _)

    // Get the top 10 nodes by incoming and outgoing edges
    val topIncoming = incomingEdges.sortBy(-_._2).take(10)
    val topOutgoing = outgoingEdges.sortBy(-_._2).take(10)

    // Print the results
    println("Top 10 nodes by incoming edges: (Node -> SumOfEdges)")
    topIncoming.foreach(pair => println(pair._1 + " -> " + pair._2))
    println("Top 10 nodes by outgoing edges: (Node -> SumOfEdges)")
    topOutgoing.foreach(pair => println(pair._1 + " -> " + pair._2))
  }
}
