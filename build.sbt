val scala3Version = "3.2.0"

val http4sVersion = "1.0.0-M37"
val fs2Version = "3.3.0"

lazy val root = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)
  .settings(
    name := "lrmi-crawler",
    version := "0.1.0-SNAPSHOT",
    resolvers ++= Resolver.sonatypeOssRepos("snapshots"),
    scalaVersion := scala3Version,
    dockerBaseImage := "openjdk:20-oracle",
    libraryDependencies ++= Seq(
      "org.apache.any23" % "apache-any23-core" % "2.7",
      "org.eclipse.rdf4j" % "rdf4j-bom" % "4.2.1",
//      "org.typelevel" %% "cats-effect" % "2.5.3",
      "org.jsoup" % "jsoup" % "1.15.3",
      "joda-time" % "joda-time" % "2.11.2",
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      "org.http4s" %% "http4s-ember-client" % http4sVersion,
      "org.scalameta" %% "munit" % "0.7.29" % Test//,
//      "org.typelevel" %% "munit-cats-effect-2" % "2.0.0-M3" % Test
    )
  )
