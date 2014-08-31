package scalaiotalk

import scala.util.Try

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._

import java.util.Date

trait Boilerplate extends Serializable {
  @transient val sparkContext: SparkContext
  @transient val dataIn: (String => String)

  def osm = {
    val csvFile = "albany.osm.pbf.csv"
    val sections_data = sparkContext.textFile(dataIn(csvFile))
    sections_data.name = "section csv"

    def hash(string: String): Long = {

      var h: Long = 1125899906842597L // prime
      val len: Long = string.length

      string.foreach(c => h = 31 * h + c)

      h
    }

    //Load data and split into nodes and id
    val vertices: VertexRDD[Array[(Long /* EdgeId */ , (Double, Double))]] = VertexRDD {
      sections_data
        .filter(s => !s.contains("coordinates,uid"))
        .map { l =>
          Try {
            val coordsString :: uuid :: Nil = l.split(",").toList
            val coords = coordsString
              .split(";")
              .map(_.trim)
              .map { x =>
                val latLong = x.replaceAll("\\s+", " ").split(" ").map(_.trim.toDouble).toList match {
                  case la :: lg :: Nil => (la, lg)
                  case a => throw new Exception("bad value: " + a)
                }

                (hash(x), latLong)
              }
            (hash(uuid), coords)
          }.toOption
        }.collect {
          case Some(x) => x
        }
    }

    //Group by node and get all connected sections
    val edges: RDD[Edge[(Double, Double)]] = {
      vertices
        .flatMap {
          case (sectionUuid, coords) =>
            coords.map {
              case (pointUuid, latLong) =>
                (pointUuid, (latLong, sectionUuid))
            }
        }
        .groupByKey // group by point's uuid
        .values // get rid of the point uuid (the EdgeId is not needed)
        .filter(_.size > 1) // remove all sections that are only crossing a single point
        .flatMap { sections =>
          sections
            .toList
            .combinations(2)
            .map(_.sortBy(_._2)) // sort by section uuid
            .flatMap {
              case x1 :: x2 :: Nil => List(Edge(x1._2, x2._2, x1._1), Edge(x2._2, x1._2, x1._1))
              case _ => throw new Exception("Should not happen!")
            }
        }
        .distinct
    }

    //Load into graph and put in cache
    val graph = Graph[Array[(Long /* EdgeId */ , (Double, Double))], (Double, Double)](vertices, edges).cache

    //Calculate page rank
    val pageRank = PageRank.run(graph, 5)
    val pageRankValues = pageRank.vertices.map(_._2)
    val minRank = pageRankValues.min
    val maxRank = pageRankValues.max

    def writeIn(to: String) = {
      val f = new java.io.File(to)
      val w = new java.io.FileWriter(f)
      val writeLine = (s: Option[String], close: Boolean) => {
        s.foreach(st => w.append(st).append("\n"))
        if (close) w.close else ()
        (f, w)
      }
      (f, writeLine)
    }

    //Get histogram of pageranks
    val histFileW = writeIn("hist.csv")

    pageRankValues
      .map(c => math.floor(100 * (c - minRank) / (maxRank - minRank)))
      .countByValue()
      .toList
      .sortBy(_._1)
      .foreach {
        case (bin, count) =>
          histFileW._2(Some(s"$bin,$count"), false)
          println(bin + "\t|\t" + ("-" * (count / 100).toInt))
      }
    val hist = histFileW._2(None, true)._1

    //should use an accumulator probably
    if (pageRank.vertices.count < 1000000 && pageRank.edges.count < 1000000) {
      val vFileW = writeIn("pg-vertices.csv")
      vFileW._2(Some("id,pg"), false)
      pageRank.vertices.collect().foreach {
        case (vId, pg) =>
          vFileW._2(Some(s"$vId,$pg"), false)
      }
      vFileW._2(None, true)._1

      val eFileW = writeIn("pg-edges.csv")
      eFileW._2(Some("source,target,lat,long,pg"), false)
      /*pageRank.*/ edges.collect().foreach {
        case Edge(sId, tId, (lat, long)) =>
          eFileW._2(Some(s"$sId,$tId,$lat,$long"), false)
      }
      eFileW._2(None, true)._1
    }
    pageRank.vertices.collect

    graph.unpersistVertices()
  }

  def flu = {
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
}

object Cluster extends Boilerplate with App {
  val master = "ec2-54-73-198-12.eu-west-1.compute.amazonaws.com"
  val sparkConf = new SparkConf(true).setMaster(s"spark://$master:7077").setAppName("scala-io")
  val hdfs = s"hdfs://$master:9000"
  val sparkContext = new SparkContext(sparkConf)
  val dataIn: String => String = (s: String) => s"$hdfs/data/$s"

  osm
  flu
}

object Local extends Boilerplate {
  @transient val sparkContext = new SparkContext(master = "local[4]", appName = "scala-io", sparkHome = "")
  @transient val dataIn: String => String = (s: String) => s"file:///home/noootsab/src/data/$s"

  def main(args: Array[String]) {
    osm
    flu
  }

}
