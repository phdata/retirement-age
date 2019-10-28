/* Copyright (c) 2018 phData inc. */

import sbt._

ThisBuild / organization := "io.phdata"
ThisBuild / scalaVersion := scalaV
ThisBuild / version := "0.1-RC2"

lazy val IntTest = config("it") extend (Test)

def itFilter(name: String): Boolean   = name endsWith "ITest"
def unitFilter(name: String): Boolean = (name endsWith "Test") && !itFilter(name)

lazy val root = (project in file("."))
  .configs(IntTest)
  .enablePlugins(OsDetectorPlugin)
  .settings(
    inConfig(IntTest)(Defaults.testTasks),
    name := "retirement-age",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.0",
    resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    libraryDependencies ++= sparkDependencies ++ otherDependencies ++
      Seq(
        "org.apache.kudu" % "kudu-binary" % kuduVersion % "it,test" classifier osDetectorClassifier.value),
    test in assembly := {},
    testOptions in Test := Seq(Tests.Filter(unitFilter)),
    testOptions in IntTest := Seq(Tests.Filter(itFilter)),
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true,
    scalafmtVersion := "1.2.0",
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    publishTo := {
      if (version.value.endsWith("SNAPSHOT"))
        Some(
          "phData Snapshots".at(
            s"$artifactoryUrl/libs-snapshot-local;build.timestamp=" + new java.util.Date().getTime))
      else
        Some("phData Releases".at(s"$artifactoryUrl/libs-release-local"))
    },
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.withClassifier(Some("assembly"))
    }
  )

addArtifact(artifact in (Compile, assembly), assembly)

val sparkVersion     = "2.2.0.cloudera1"
val scalaTestVersion = "3.0.4"
val kuduVersion      = "1.9.0"
val artifactoryUrl   = "https://repository.phdata.io/artifactory"

parallelExecution in Test := false
val scalaV = "2.11.8"
val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
).map(
  _.excludeAll(
    ExclusionRule(organization = "org.glassfish.jersey.core", name = "jersey-client"),
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
  ))
val otherDependencies = Seq(
  "org.rogach"                 %% "scallop"         % "3.1.1",
  "net.jcazevedo"              %% "moultingyaml"    % "0.4.0",
  "com.google.guava"           % "guava"            % "21.0",
  "com.typesafe.scala-logging" %% "scala-logging"   % "3.7.2",
  "org.scalatest"              %% "scalatest"       % scalaTestVersion % "it,test",
  "com.databricks"             %% "spark-avro"      % "4.0.0",
  "org.apache.kudu"            %% "kudu-spark2"     % kuduVersion,
  "org.apache.kudu"            % "kudu-test-utils"  % kuduVersion % "it,test",
  "org.slf4j"                  % "log4j-over-slf4j" % "1.7.26",
  "ch.qos.logback"             % "logback-classic"  % "1.2.3"
).map(
  _.excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"),
               ExclusionRule(organization = "org.apache.logging.log4j"),
               ExclusionRule(organization = "log4j")))
