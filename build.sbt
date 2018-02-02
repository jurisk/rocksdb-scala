name := "rocksdb-scala"

version := "0.1"

scalaVersion := "2.12.4"

scalacOptions in Test ++= Seq("-Yrangepos")

val akkaVersion = "2.5.9"

libraryDependencies ++= Seq(
  "org.rocksdb" % "rocksdbjni" % "5.9.2",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "org.scalactic" %% "scalactic" % "3.0.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)
