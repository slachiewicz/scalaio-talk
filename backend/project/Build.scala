import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import org.scalastyle.sbt._
import com.typesafe.sbt.SbtScalariform

object MyBuild extends Build {

  lazy val sharedSettings = {
    Project.defaultSettings ++ 
    ScalastylePlugin.Settings ++ 
    Seq(assemblySettings: _*) ++ 
    SbtScalariform.scalariformSettings ++ 
    Seq(
      name := "Scala IO Talk",
      description := "The scala io talk on graphx and large network analysis",
      version := "0.1",
      scalaVersion := "2.10.4",

      libraryDependencies ++= Seq(
        
        //Testing and logging
        "com.typesafe" %% "scalalogging-slf4j" % "1.1.0",
        "org.scalatest" % "scalatest_2.10" % "2.2.0-M1" % "test",
        
        //Spray
        // "io.spray" % "spray-client" % "1.3.1",
        // "io.spray" %% "spray-json" % "1.2.6",
        // "io.spray" % "spray-routing" % "1.3.1",
        // "io.spray" % "spray-testkit" % "1.3.1",
        
        // //Akka
        // "com.typesafe.akka" %% "akka-actor" % "2.3.3",
        // "com.typesafe.akka" %% "akka-testkit" % "2.3.3",
        // "com.typesafe.akka" % "akka-slf4j_2.10" % "2.3.3",
        
        // //Joda time
        // "joda-time" % "joda-time" % "2.3",
        // "org.joda" % "joda-convert" % "1.6",
        
        // //Geo utils
        // "com.meetup" %% "archery" % "0.3.0",
        // "com.github.hjast" % "geodude_2.10" % "0.2.0", 
        // "com.graphhopper" % "graphhopper" % "0.3",

        //Spark
        "org.apache.spark" % "spark-core_2.10" % "1.0.2",
        "org.apache.spark" % "spark-graphx_2.10" % "1.0.2"
      ),

      resolvers ++= Seq(
        Opts.resolver.sonatypeSnapshots,
        Opts.resolver.sonatypeReleases,
        "Clojars Repository" at "http://clojars.org/repo",
        "Adam Gent Maven Repository" at "http://mvn-adamgent.googlecode.com/svn/maven/release",
        "opengeo" at "http://repo.opengeo.org/",
        "osgeo" at "http://download.osgeo.org/webdav/geotools/",
        "Akka Repository" at "http://repo.akka.io/releases/",
        "Spray repo" at "http://repo.spray.io/",
        "bintray/meetup" at "http://dl.bintray.com/meetup/maven",
        "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
        "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
      ),
              
      parallelExecution in Test := false, // until scalding 0.9.0 we can't do this
            
      scalacOptions ++= Seq(
          "-unchecked",
          "-deprecation",
          "-feature",
          "-language:implicitConversions",
          "-language:reflectiveCalls",
          "-language:postfixOps",
          "-Yresolve-term-conflict:package"
      ),

      javaOptions += "-Xmx13G",
      javaOptions += "-Dconfig.resources=../lol-dev.conf",

      publishMavenStyle := true,
      pomIncludeRepository := { x => false },

      mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
        {
          //case m if m.toLowerCase.matches(".*akka/remote/transport/netty/.*") => MergeStrategy.first // Akka fix
          case m if m.toLowerCase.matches("meta-inf.*") => MergeStrategy.discard
          case m if m.toLowerCase.matches("log4j.properties") => MergeStrategy.discard
          case m if m.toLowerCase.matches("application.conf") => MergeStrategy.concat
          case m if m.toLowerCase.matches("reference.conf") => MergeStrategy.concat
          //case x => old(x)
          case _ => MergeStrategy.first
        }
      }
    )
  }
  
  def module(name: String, dir: Option[String] = None) = {
      val id = "scalaiotalk-%s".format(name)
      val fdir = "scalaiotalk-%s".format(dir.getOrElse(name))
      Project(id = id, base = file(fdir), settings = sharedSettings ++ Seq(Keys.name := id))
  }

  lazy val mining = module("mining").settings(
    mainClass in (Compile, run) := Some("scalaiotalk.Main"),
    mainClass in assembly := Some("scalaiotalk.Main"),
    jarName in assembly := "scalaiotalk-mining.jar"
  )

}
