
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
 * Show some basic graph operations.
 *
 */
object Example1 {

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    val sc = spark.sparkContext
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Seq(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    println(graph)

    println()
    graph.vertices.foreach(println(_))
    graph.edges.foreach(println(_))
    println()

    val inDegrees: VertexRDD[Int] = graph.inDegrees
    println("in degrees");
    inDegrees.collect.foreach(println(_))
    println()

    val outDegrees: VertexRDD[Int] = graph.outDegrees
    println("out degrees");
    outDegrees.collect.foreach(println(_))
    println()

    // Count all the edges where src > dst
    val count = graph.edges.filter(e => e.srcId > e.dstId).count
    println("#(u, v) with u > v:"  + count)
    println()


    val facts: RDD[String] = graph.triplets.map(triplet =>
      triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
    println()

    val reverse = graph.reverse
    println()
    println("--reverse graph---")
    reverse.vertices.foreach(println(_))
    reverse.edges.foreach(println(_))
    println()

    val reverseInDegrees: VertexRDD[Int] = reverse.inDegrees
    println("in degrees");
    reverseInDegrees.collect.foreach(println(_))
    println()

    val reverseOutDegrees: VertexRDD[Int] = reverse.outDegrees
    println("out degrees");
    reverseOutDegrees.collect.foreach(println(_))
    println()


    spark.stop()
  }
}
