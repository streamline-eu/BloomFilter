name := "combined-occurrences"

version := "0.1.0"

scalaVersion := "2.11.7"

lazy val flinkVersion = "1.2.0"

lazy val commonDependencies = Seq(
  "com.github.melrief" % "purecsv_2.11" % "0.1.0",
  "junit" % "junit" % "4.12" % "test",
  "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
)

lazy val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion,
  "org.apache.flink" %% "flink-test-utils" % flinkVersion
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies,
    libraryDependencies ++= flinkDependencies.map(_ % /*"compile"*/"provided")
  )

lazy val commonSettings = Seq(
  organization := "hu.sztaki.ilab",
  version := "0.1.0",
  scalaVersion := "2.11.7",
  test in assembly := {}
)

