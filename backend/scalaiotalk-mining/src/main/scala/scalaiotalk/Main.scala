//package scalaiotalk

//object Main extends App {

  import org.apache.spark._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.SparkContext._
  import org.apache.spark.graphx._

  // val master = "ec2-54-73-198-12.eu-west-1.compute.amazonaws.com"
  // val sparkConf = new SparkConf(true).setMaster(s"spark://$master:7077").setAppName("scala-io")
  // val sc = new SparkContext(sparkConf)

  val hdfs = s"hdfs://ec2-54-73-198-12.eu-west-1.compute.amazonaws.com:9000"

  val london = sc.textFile(s"$hdfs/data/london/london_sections.csv")
  london.name = "london csv"

  def hash(string: String): Long = {
    
    var h: Long = 1125899906842597L // prime
    val len: Long = string.length

    string.foreach(c => h = 31*h + c)

    h
  }
  
  //Load data and split into nodes and id
  val data: RDD[(Long, Array[Long])] = {
    london
    .filter(s => !s.contains("coordinates,uid"))
    .map { l =>
      val coordsString :: uuid :: Nil = l.split(",").toList
      val coords = coordsString
        .split(";")
        .map(_.trim.replaceAll("\\s+", "_"))
        .map(hash)
      (hash(uuid), coords)
    }
    .cache
  }

  //Group by node and get all connected sections
  val edges: RDD[(VertexId, VertexId)] = {
    data
      .flatMap { case (uuid, coords) => coords.map(c => (c, uuid)) }
      .groupByKey
      .values
      .filter(_.size > 1)
      .flatMap { sections =>
        sections
          .toList
          .combinations(2)
          .map(_.sorted)
          .flatMap {
            case x1 :: x2 :: Nil => List((x1, x2), (x2, x1))
            case _ => throw new Exception("Should not happen!")
          }
      }
      .distinct
  }

  //Load into graph
  val graph = Graph.fromEdgeTuples(edges, 0)

  graph.inDegrees.take(5).foreach(println)

//}
