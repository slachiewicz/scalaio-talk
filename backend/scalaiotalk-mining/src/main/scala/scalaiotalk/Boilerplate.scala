package scalaiotalk

import scala.util.Try

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._

import java.util.Date

object Util {
  type SectionId = Long
  type PointId = Long
  type LatLong = (Double, Double)
  type SectionData = Array[(PointId, LatLong)]
  type Cross = Iterable[(LatLong, SectionId)]

  def hash(string: String): Long = {
    var h: Long = 1125899906842597L // prime
    val len: Long = string.length
    string.foreach(c => h = 31 * h + c)
    h
  }
}

import Util._

class OSM(csvFile: String)(implicit sparkContext: SparkContext, dataIn: (String => String)) {

  lazy val rawSections: RDD[String] = {
    val sectionsDataString = sparkContext.textFile(dataIn(csvFile))
    sectionsDataString.name = "section csv"
    sectionsDataString
  }

  // VertexRDD[T] is actually a RDD[VertexId, T]
  // in this case the VertexId is the SectionId (graph's dual)
  lazy val vertices: VertexRDD[SectionData] = {
    val stringToLatLong = (x: String) => {
      val clean = x.replaceAll("\\s+", " ")
      val latLongList: List[Double] = clean.split(" ").map(_.trim.toDouble).toList
      val latLong: LatLong = latLongList match {
        case la :: lg :: Nil => (la, lg)
        case a => throw new Exception("bad value: " + a)
      }
      latLong
    }

    val stringToSectionData = (coordsString: String) => {
      val coordsList: Array[String] = coordsString.split(";").map(_.trim)
      val section: SectionData = coordsList.map { x =>
        val latLong: LatLong = stringToLatLong(x)
        // keep the hash of the line as identifier of the POINT
        //  → better (more efficient) than a string
        // latLong is the coordinate of the vertex
        val vertexId: PointId = hash(x)
        (vertexId, latLong)
      }
      section
    }

    val vertexData =
      rawSections
        .filter(s => !s.contains("coordinates,uid")) // skip header line
        .map { l =>
          Try {
            val coordsString :: uuid :: bool :: Nil = l.split(",").toList
            val section = stringToSectionData(coordsString)
            // keep the hash of the section's uuid (same reason as for the point)
            // coords is the list of coordinates participating to this section
            val sectionId: SectionId = hash(uuid)
            (sectionId, section)
          }.toOption
        }.collect {
          case Some(x) => x
        }

    val vertexRdd = VertexRDD { vertexData }
    vertexRdd
  }

  lazy val edges: RDD[Edge[LatLong]] = {
    val insideOut: ((VertexId, SectionData)) => TraversableOnce[(PointId, (LatLong, SectionId))] = (v: (VertexId, SectionData)) => {
      val (sectionId, coords) = v
      coords.map {
        case (pointUuid, latLong) =>
          (pointUuid, (latLong, sectionId))
      }
    }
    val points: RDD[(PointId, (LatLong, SectionId))] = vertices flatMap insideOut

    val crossingPoints: RDD[Cross] = points.groupByKey // group by point's uuid
      // get rid of the point uuid (the PointId is not needed)
      .values
      // remove all sections that are only crossing a single point
      .filter(_.size > 1)

    crossingPoints.flatMap { sections =>
      val pairOfCrossPoints = sections.toList.combinations(2)
      pairOfCrossPoints.map(_.sortBy(_._2)) // sort by section id => to avoid duplicates
        .flatMap {
          case (latLong, sectinoId1) :: (_, sectinoId2) :: Nil =>
            // *** /!\ *** latLongs are actually the same
            List(
              Edge(sectinoId1, sectinoId2, latLong),
              Edge(sectinoId2, sectinoId1, latLong)
            )
          case _ =>
            throw new Exception("Should not happen!")
        }
    }
    // ↓↓↓ SHOULDN'T BE NEEDED ANYMORE
    //.distinct
  }

  lazy val graph = Graph[Array[(SectionId, LatLong)], LatLong](vertices, edges)

  //Calculate page rank
  lazy val pageRank = PageRank.run(graph, 5)
  lazy val pageRankValues = pageRank.vertices.map(_._2)
  lazy val minRank = pageRankValues.min
  lazy val maxRank = pageRankValues.max

  lazy val pageRankHist: List[String] = {
    pageRankValues
      .map(c => math.floor(100 * (c - minRank) / (maxRank - minRank)))
      .countByValue()
      .toList
      .sortBy(_._1)
      .map {
        case (bin, count) => s"$bin,$count"
      }
  }

  // pageRanks (vertices) and all edges
  lazy val asStrings: (RDD[String], RDD[String]) = {
    //pg-vertices.csv
    val v = pageRank.vertices.map {
      case (vId, pg) => s"$vId,$pg"
    }

    //pg-edges.csv
    val e = edges.map {
      case Edge(sId, tId, (lat, long)) =>
        s"$sId,$tId,$lat,$long"
    }
    (v, e)
  }

  lazy val run: Unit = {
    def time(msg: String, b: => Any) {
      val now = new java.util.Date()
      val m = s">>> $msg ($now)"
      println("*" * m.size)
      println(m)
      b
      val end = new java.util.Date()
      println(s"$msg ($end → took ${end.getTime - now.getTime} ms)<<<")
      println("-" * m.size)
    }
    time(s"Registering $csvFile as an RDD of Serializabletring", rawSections)
    println(s"###  Number of lines ${rawSections.count}")

    time("Creating RDD of vertices", vertices)
    println(s"###  Number of vertices ${vertices.count}")

    time("Creating RDD of edges", edges)
    println(s"###  Number of edges ${edges.count}")

    time("Creating the graph", graph)

    time("Computing the pageRank", pageRank)
    println(s"###  Number of vertices in pageRank result ${pageRank.vertices.count}")
  }
}

trait Boilerplate extends Serializable {
  implicit def sparkContext: SparkContext
  implicit def dataIn: String => String

  var csvFile: String = "albany.osm.pbf.csv"

  lazy val osm = {
    val o = new OSM(csvFile)(sparkContext, dataIn)
    o.run
    o
  }

  lazy val flu = {
    val DATE_TMPL = new java.text.SimpleDateFormat("yyyy-MM-dd")
    def toDate(s: String) = DATE_TMPL.parse(s)
    // on all countries
    val countries = List(
      "za", "de", "ar", "au", "at", "be", "bo", "br", "bg", "ca", "cl", "es", "us",
      "fr", "hu", "jp", "mx", "no", "nz", "py", "nl", "pe", "pl", "ro", "ru", "se",
      "ch", "ua", "uy"
    )
    for (country <- countries) {
      val countryFile = dataIn(s"flutrends/$country-flu.csv")
      val file = sparkContext.textFile(countryFile).cache

      val header = file.filter(_.startsWith("Date")).first()

      val cities = header.split(",").map(_.trim).toList.drop(2) //drop country

      val survey: RDD[(String, (Date, Double))] =
        file.filter(_ != header).flatMap { l =>
          val date :: xs = l.split(",").map(_.trim).toList
          xs.drop(1).zip(cities).map {
            case (x, c) =>
              (c, (toDate(date), if (x == "") 0d else x.toDouble))
          }
        }

      val byCity = survey.groupByKey.mapValues { vs =>
        vs.toList.sortBy(_._1)
      }.map(identity)

      val allCities = sparkContext.union(byCity)

    }
  }

  def exec(args: Array[String]) {
    args.headOption.foreach(x => csvFile = x)

    val (pg, es) = osm.asStrings
    val id = java.util.UUID.randomUUID.toString
    val pgFile = dataIn(s"page-rank-result-$id.csv")
    val esFile = dataIn(s"graph-edges-$id.csv")
    println(s"Saving ${pg.count} page rank data to file $pgFile ")
    pg.saveAsTextFile(pgFile)
    println(s"Saving ${es.count} graph edges to file $esFile")
    es.saveAsTextFile(esFile)
    println("DONE")
    //flu
  }

}

object Cluster extends Boilerplate {
  val master = "ec2-54-73-198-12.eu-west-1.compute.amazonaws.com"
  lazy val sparkConf = new SparkConf(true).setMaster(s"spark://$master:7077").setAppName("scala-io")
  val hdfs = s"hdfs://$master:9000"
  lazy val sparkContext = new SparkContext(sparkConf)
  override implicit val dataIn: String => String = (s: String) => s"$hdfs/data/$s"
}

object Local extends Boilerplate {
  val sparkContext = new SparkContext(master = "local[4]", appName = "scala-io", sparkHome = "")
  val dataIn: String => String = (s: String) => s"file:///home/noootsab/src/data/$s"
  def main(args: Array[String]) = exec(args)
}
