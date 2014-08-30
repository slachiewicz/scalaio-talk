package scalaiotalk

object Main extends App {

  val master = "ec2-54-77-113-86.eu-west-1.compute.amazonaws.com"

  val hdfs = s"hdfs://$master:9000"
  
  val london = sc.textFile(s"$hdfs/data/london/london_sections.csv")
  london.name = "london csv"
  val londonCsv = london.cache
  
  val points = londonCsv.filter(s => !s.contains("coordinates,uid")).flatMap { l =>
    val coordsString::uuid::Nil = l.split(",").toList
    val coords = coordsString
                  .split(";")
                  .map(_.trim.replaceAll("\\s+", "_").hashCode)
    coords.map(c => (c, uuid))
  }
  
  
  val sections = points.groupByKey
                        .values
                          .filter(_.size > 1)
                          .flatMap { sections =>
                            sections.toList.combinations(2)
                                            .map(_.sorted)
                                            .flatMap{ 
                                              case x1::x2::Nil =>  List(x1::x2::Nil, x2::x1::Nil) 
                                            }
                          }
                          .distinct

}
