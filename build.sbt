import sbtassembly.AssemblyPlugin.autoImport._

// 2.12 to match the scala-library bundled in the spark 3.5.x distribution
// (apache/spark images and most enterprise spark builds are scala 2.12)
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.github.siwarattia"

val sparkVersion     = "3.5.3"
val hadoopAwsVersion = "3.3.4"

// keep hadoop-aws at the exact hadoop version the spark build uses (3.5.x -> 3.3.4)
val baseSettings = Seq(
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
  Test / fork := true
)

// settings shared by every runnable job module (spark-submit targets)
val jobSettings = Seq(
  libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "reference.conf"              => MergeStrategy.concat
    case _                             => MergeStrategy.first
  }
)

lazy val common = (project in file("modules/common"))
  .settings(
    baseSettings,
    name := "datahub-common",
    libraryDependencies ++= Seq(
      "com.typesafe"      %  "config"     % "1.4.3",
      "org.apache.hadoop" %  "hadoop-aws" % hadoopAwsVersion,
      "org.apache.spark"  %% "spark-sql"  % sparkVersion % Provided
    )
  )

lazy val ingestion = (project in file("modules/ingestion"))
  .dependsOn(common)
  .enablePlugins(AssemblyPlugin)
  .settings(
    baseSettings,
    jobSettings,
    name := "datahub-ingestion",
    Compile / mainClass := Some("io.github.siwarattia.datahub.ingestion.RawIngestionJob"),
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar"
  )

lazy val transform = (project in file("modules/transform"))
  .dependsOn(common)
  .enablePlugins(AssemblyPlugin)
  .settings(
    baseSettings,
    jobSettings,
    name := "datahub-transform",
    Compile / mainClass := Some("io.github.siwarattia.datahub.transform.CuratedTransactionsJob"),
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar"
  )

lazy val quality = (project in file("modules/quality"))
  .dependsOn(common)
  .enablePlugins(AssemblyPlugin)
  .settings(
    baseSettings,
    jobSettings,
    name := "datahub-quality",
    Compile / mainClass := Some("io.github.siwarattia.datahub.quality.DataQualityJob"),
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar"
  )

lazy val root = (project in file("."))
  .aggregate(common, ingestion, transform, quality)
  .settings(
    name := "banking-datahub",
    publish / skip := true
  )
