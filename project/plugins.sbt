resolvers += "phData Snapshots" at "https://repository.phdata.io/artifactory/libs-snapshot"
classpathTypes += "maven-plugin"
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")
addSbtPlugin("com.lucidchart" % "sbt-scalafmt-coursier" % "1.12")
addSbtPlugin("io.phdata" % "sbt-os-detector" % "0.1.0-SNAPSHOT")
