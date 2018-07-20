/* Copyright (c) 2018 phData inc. */

import sbt._

lazy val IntegrationTest = config("it") extend (Test)
lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .settings(
    name := "retirement-age",
    version := "0.1-SNAPSHOT",
    organization := "io.phdata",
    scalaVersion := scalaV,
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.0",
    resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    libraryDependencies ++= sparkDependencies ++ otherDependencies,
    test in assembly := {},
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true,
    scalafmtVersion := "1.2.0"
  )

val sparkVersion     = "2.2.0.cloudera1"
val scalaTestVersion = "3.0.4"

parallelExecution in Test := false
val scalaV = "2.11.8"
val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
).map(_.exclude("org.glassfish.jersey.core", "jersey-client"))
val otherDependencies = Seq(
  "org.rogach"                 %% "scallop"       % "3.1.1",
  "net.jcazevedo"              %% "moultingyaml"  % "0.4.0",
  "com.google.guava"           % "guava"          % "21.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.scalatest"              %% "scalatest"     % "3.0.4" % "test",
  "com.databricks"             %% "spark-avro"    % "4.0.0"
).map(_.excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core")))
