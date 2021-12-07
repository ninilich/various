name := "example-project"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided"
)

assembly / mainClass := Some("myexampleproject.tasks")
