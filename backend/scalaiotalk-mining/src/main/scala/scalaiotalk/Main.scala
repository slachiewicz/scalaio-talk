package scalaiotalk

object Main extends App {

  def run(csvFile: String) {

    import org.apache.spark._
    import org.apache.spark.rdd.RDD
    import org.apache.spark.SparkContext._
    import org.apache.spark.graphx._
    import org.apache.spark.graphx.lib._

    val master = "ec2-54-73-198-12.eu-west-1.compute.amazonaws.com"
    val sparkConf = new SparkConf(true).setMaster(s"spark://$master:7077").setAppName("scala-io")
    val sc = new SparkContext(sparkConf)

    val hdfs = s"hdfs://$master:9000"

    val sections_data = sc.textFile(s"$hdfs/$csvFile")
    sections_data.name = "section csv"

    def hash(string: String): Long = {

      var h: Long = 1125899906842597L // prime
      val len: Long = string.length

      string.foreach(c => h = 31 * h + c)

      h
    }

    //Load data and split into nodes and id
    val data: RDD[(Long, Array[Long])] = {
      sections_data
        .filter(s => !s.contains("coordinates,uid"))
        .map { l =>
          val coordsString :: uuid :: Nil = l.split(",").toList
          val coords = coordsString
            .split(";")
            .map(_.trim.replaceAll("\\s+", "_"))
            .map(hash)
          (hash(uuid), coords)
        }
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

    //Calculate page rank and put in cache
    val pageRank = PageRank.run(graph, 5).vertices.map(_._2)
    val minRank = pageRank.min
    val maxRank = pageRank.max

    println(minRank + " " + maxRank)

    //Get histogram of pageranks
    pageRank
      .map(c => math.floor(100 * (c - minRank) / (maxRank - minRank)))
      .countByValue()
      .toList
      .sortBy(_._1)
      .foreach {
        case (bin, count) =>
          println(bin + "\t|\t" + ("-" * (count / 100).toInt))
      }

    pageRank.unpersist()

    //Page rank
    //Centr
  }

  run("data/london_sections.csv")

}
