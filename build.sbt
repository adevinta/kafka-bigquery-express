import sbt.*

import java.time.*
import scala.sys.process.*
import com.typesafe.sbt.packager.docker.DockerAlias

lazy val dockerRegistry = sys.env.get("ARTIFACTORY_DOCKER_REGISTRY")
lazy val releaseVersion = sys.env.get("RELEASE_VERSION")

val PrometheusVersion = "0.16.0"
val JsoniterVersion = "2.30.9"
val ZioVersion = "2.1.9"
val ZioKafkaVersion = "2.8.2"

ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.adevinta"
ThisBuild / version := "1.0"

lazy val `kafka-bigquery` = project
  .in(file("."))
  .aggregate(
    shared,
    `zio-common-http`,
    `zio-common-metrics`,
    `zio-common-util`,
    `bq-writer`,
    `gcs-writer`,
  )
  .settings(
    publish / skip := true,
    crossScalaVersions := Nil
  )

lazy val shared = project
  .enablePlugins(
    JavaAppPackaging,
  )
  .dependsOn(`zio-common-util`)
  .settings(addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % ZioVersion,
      "dev.zio" %% "zio-streams" % ZioVersion,

      // GCP
      "com.google.cloud" % "google-cloud-storage" % "2.42.0",
      // needed because "google-cloud-storage" % "2.30.2" depends on grpc 1.60.0:
      "io.grpc" % "grpc-alts" % "1.66.0",
      "io.grpc" % "grpc-auth" % "1.66.0",
      "io.grpc" % "grpc-googleapis" % "1.66.0",
      "io.grpc" % "grpc-inprocess" % "1.66.0",
      "io.grpc" % "grpc-rls" % "1.66.0",
      // end
      "io.grpc" % "grpc-netty" % "1.66.0" exclude ("io.netty", "netty-codec-http2"),
      "io.netty" % "netty-codec-http2" % "4.1.113.Final",
      "io.netty" % "netty-codec-socks" % "4.1.113.Final",
      "io.netty" % "netty-handler-proxy" % "4.1.113.Final",
      // needed because zio-http depends on an older version
      "io.netty" % "netty-transport-native-epoll" % "4.1.113.Final",
      "io.netty" % "netty-transport-native-kqueue" % "4.1.113.Final",
      // end

      // test dependencies
      "dev.zio" %% "zio-test" % ZioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % ZioVersion % Test,
      "dev.zio" %% "zio-mock" % "1.0.0-RC12" % Test,
      "com.softwaremill.quicklens" %% "quicklens" % "1.9.8" % Test,
    ),
    publish / skip := true,
  )

lazy val `zio-common-http` = project
  .dependsOn(`zio-common-metrics`)
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % ZioVersion,
      "dev.zio" %% "zio-http" % "3.0.0",
      "dev.zio" %% "zio-test" % ZioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % ZioVersion % Test,
    ),
  )

lazy val `zio-common-metrics` = project
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % ZioVersion,
      "io.prometheus" % "simpleclient" % PrometheusVersion,
      "io.prometheus" % "simpleclient_common" % PrometheusVersion,
      "io.prometheus" % "simpleclient_hotspot" % PrometheusVersion,
      "dev.zio" %% "zio-test" % ZioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % ZioVersion % Test,
    ),
  )

lazy val `zio-common-util` = project
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % ZioVersion,
      "dev.zio" %% "zio-test" % ZioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % ZioVersion % Test,
    ),
  )

lazy val `bq-writer` = project
  .enablePlugins(
    AshScriptPlugin,
    BuildInfoPlugin,
    JavaAppPackaging,
    DockerPlugin,
  )
  .dependsOn(shared % "compile->compile;test->test")
  .dependsOn(`zio-common-http`)
  .dependsOn(`zio-common-metrics`)
  .settings(buildInfoSettings)
  .settings(dockerSettings)
  .settings(addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
  .settings(
    resolvers += "confluent" at "https://packages.confluent.io/maven/",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-streams" % ZioVersion,

      // Google Cloud Dependencies
      "com.google.cloud" % "google-cloud-storage" % "2.36.1",
      "com.google.cloud" % "google-cloud-bigquery" % "2.42.3",

      // Jsoniter
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % JsoniterVersion,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % JsoniterVersion % Provided,

      // Logging
      "org.slf4j" % "slf4j-api" % "2.0.16",
      "ch.qos.logback" % "logback-classic" % "1.5.8" % Runtime,
      "net.logstash.logback" % "logstash-logback-encoder" % "8.0" % Runtime,
      "dev.zio" %% "zio-logging" % "2.3.1",

      // test dependencies
      "dev.zio" %% "zio-test" % ZioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % ZioVersion % Test,
      "dev.zio" %% "zio-mock" % "1.0.0-RC12" % Test,
      "com.softwaremill.quicklens" %% "quicklens" % "1.9.8" % Test,
    ),
    Compile / mainClass := Some("com.adevinta.bq.bqwriter.App"),
    publish / skip := true,
  )
  .settings(jacksonOverrides)

lazy val `gcs-writer` = project
  .enablePlugins(
    AshScriptPlugin,
    BuildInfoPlugin,
    JavaAppPackaging,
    DockerPlugin,
  )
  .dependsOn(shared % "compile->compile;test->test")
  .dependsOn(`zio-common-http`)
  .dependsOn(`zio-common-util`)
  .settings(buildInfoSettings)
  .settings(dockerSettings)
  .settings(addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
  .settings(
    resolvers += "confluent" at "https://packages.confluent.io/maven/",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-streams" % ZioVersion,
      "dev.zio" %% "zio-kafka" % ZioKafkaVersion,
      "io.confluent" % "kafka-schema-registry-client" % "7.7.1" excludeAll ("org.apache.kafka", "kafka-clients"),
      "org.apache.kafka" % "kafka-clients" % "3.8.0",

      // Logging
      "org.slf4j" % "slf4j-api" % "2.0.16",
      "ch.qos.logback" % "logback-classic" % "1.5.8" % Runtime,
      "net.logstash.logback" % "logstash-logback-encoder" % "8.0" % Runtime,
      "dev.zio" %% "zio-logging" % "2.3.1",

      // Needed because versions depended on have vulnerabilities:
      "org.apache.commons" % "commons-compress" % "1.27.1",

      // test dependencies
      "com.chuusai" %% "shapeless" % "2.3.12" % Test,
      "dev.zio" %% "zio-test" % ZioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % ZioVersion % Test,
      "dev.zio" %% "zio-mock" % "1.0.0-RC12" % Test,
      "dev.zio" %% "zio-kafka-testkit" % ZioKafkaVersion % Test,
      "io.github.embeddedkafka" %% "embedded-kafka" % "3.8.0" % Test,
      "com.softwaremill.quicklens" %% "quicklens" % "1.9.8" % Test,
    ),
    Compile / mainClass := Some("com.adevinta.bq.gcswriter.App"),
    publish / skip := true,
  )
  .settings(jacksonOverrides)

// Jackson is a transitive dependency of many libraries.
lazy val jacksonOverrides =
  dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % "2.17.0",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.0",
    "com.fasterxml.jackson.core" % "jackson-annotations" % "2.17.1",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.17.0",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.17.0",
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.17.1",
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.17.0",
    "com.fasterxml.jackson.datatype" %% "jackson-datatype-module-scala" % "2.17.0",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.0"
  )

ThisBuild / scalacOptions += "-deprecation"
ThisBuild / testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
// No scaladoc generation
ThisBuild / Compile / doc / sources := Seq.empty

// --------------------------------------
// Settings for publishing Docker images
// --------------------------------------
val dockerSettings = {
  val dockerUser = "dockerUser"
  val shaBasedTag = s"${gitSha}_${System.currentTimeMillis() / 1000}"

  Seq(
    dockerAliases := {
      val dockerAlias = DockerAlias(dockerRegistry, Some(dockerUser), name.value, None)
      Seq(
        Some(dockerAlias.copy(tag = Some(shaBasedTag))),
        releaseVersion.map(v => dockerAlias.copy(tag = Some(v))),
        if (onDefaultBranch) Some(dockerAlias.copy(tag = Some("latest"))) else None
      ).flatten
    },
    Docker / daemonUserUid := None,
    Docker / daemonUser := "daemon",
    dockerBaseImage := "amazoncorretto:21-al2-jdk",
    dockerExposedPorts ++= Seq(8888),
    Universal / javaOptions ++= Seq(
      "-J-XX:+UseContainerSupport",
      "-J-XX:MinRAMPercentage=75.0",
      "-J-XX:MaxRAMPercentage=75.0",
      "-J-XX:+HeapDumpOnOutOfMemoryError",
      "-J-XX:HeapDumpPath=/tmp/dump.hprof"
    ),
  )
}

lazy val buildInfoSettings = List(
  // BuildInfo, get it with the "/version" route
  buildInfoKeys := Seq[BuildInfoKey](
    "releaseVersion" -> releaseVersion.getOrElse("dev"),
    "gitSha" -> gitSha,
    "builtAt" -> ZonedDateTime.now
      .withZoneSameInstant(ZoneId.of("Europe/Amsterdam"))
      .toString
  ),
  buildInfoOptions += BuildInfoOption.ToJson,
  buildInfoPackage := "com.adevinta.bigquery",
)

// Avoid sbt-dynver to use the default `+` separator and generate a valid version to be used as Docker tag
ThisBuild / dynverSeparator := "-"

// ------------------------------------------------------------------------------------------
// COMMAND ALIASES
// ------------------------------------------------------------------------------------------
addCommandAlias("fmt", ";scalafmtSbt; scalafmtAll")
addCommandAlias("fmtCheck", ";scalafmtSbtCheck; scalafmtCheckAll")
addCommandAlias("check", ";scalafmtSbtCheck; scalafmtCheckAll; Test/compile")

// ------------------------------------------------------------------------------------------
// Utilities
// ------------------------------------------------------------------------------------------

lazy val gitSha: String =
  "git describe --tags --always --dirty".!!.trim

lazy val gitBranch: String =
  "git rev-parse --abbrev-ref HEAD".!!.trim

def onDefaultBranch: Boolean =
  Set("main", "master").contains(gitBranch)
