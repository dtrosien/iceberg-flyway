// sbt 2 build: bare settings are common settings and apply to every subproject,
// so no `ThisBuild /` scoping is needed.
// `version` is intentionally not set: sbt-ci-release derives it from git tags (sbt-dynver).
scalaVersion  := "3.9.0"
organization  := "com.example"
versionScheme := Some("early-semver")

// Publishing metadata used by sbt-ci-release.
homepage   := Some(uri("https://github.com/dtrosien/iceberg-flyway"))
licenses   := List(License.Apache2)
developers := List(
  Developer(
    "dtrosien",
    "Dominik Trosien",
    "dominiktrosien@yahoo.de",
    uri("https://github.com/dtrosien")
  )
)

val catsVersion           = "2.13.0"
val declineVersion        = "2.6.2"
val scalatestVersion      = "3.2.19"
val pureconfigVersion     = "0.17.10"
val flywayVersion         = "12.10.0"
val icebergVersion        = "1.11.0"
val hadoopVersion         = "3.4.3"
val rowdata4sVersion      = "0.2.0"
val flinkVersion          = "1.20.3"
val postgresVersion       = "42.7.10"
val testcontainersVersion = "2.0.5"
val osLibVersion          = "0.11.8"
val logbackVersion        = "1.5.38"

lazy val root = rootProject
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    name := "iceberg-flyway",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:all"
    ),
    libraryDependencies ++= Seq(
      "org.typelevel"         %% "cats-core"       % catsVersion,
      "com.monovore"          %% "decline"         % declineVersion,
      "com.github.pureconfig" %% "pureconfig-core" % pureconfigVersion,
      // Flyway (Apache 2.0 modules only) keeps its history table in Postgres.
      "org.flywaydb"   % "flyway-core"                % flywayVersion,
      "org.flywaydb"   % "flyway-database-postgresql" % flywayVersion,
      "org.postgresql" % "postgresql"                 % postgresVersion,
      // Iceberg Java API; iceberg-flink provides FlinkSchemaUtil to turn the
      // Flink DataType derived by rowdata4s into an Iceberg Schema.
      "org.apache.iceberg"  % "iceberg-core"       % icebergVersion,
      "org.apache.iceberg"  % "iceberg-data"       % icebergVersion,
      "org.apache.iceberg"  % "iceberg-parquet"    % icebergVersion,
      "org.apache.iceberg"  % "iceberg-flink-1.20" % icebergVersion,
      "io.github.dtrosien" %% "flink-rowdata4s"    % rowdata4sVersion,
      // rowdata4s declares Flink as provided; the consuming project supplies it.
      "org.apache.flink" % "flink-table-api-java" % flinkVersion,
      // Glue catalog and S3FileIO; the bundle ships the shaded AWS SDK they need.
      "org.apache.iceberg" % "iceberg-aws"        % icebergVersion,
      "org.apache.iceberg" % "iceberg-aws-bundle" % icebergVersion,
      // parquet-hadoop needs Hadoop's Configuration on the classpath.
      "org.apache.hadoop"  % "hadoop-client-api"         % hadoopVersion,
      "org.apache.hadoop"  % "hadoop-client-runtime"     % hadoopVersion         % Runtime,
      "org.scalatest"     %% "scalatest"                 % scalatestVersion      % Test,
      "com.lihaoyi"       %% "os-lib"                    % osLibVersion          % Test,
      "org.testcontainers" % "testcontainers-postgresql" % testcontainersVersion % Test,
      "ch.qos.logback"     % "logback-classic"           % logbackVersion
    ),
    Compile / mainClass := Some("com.example.icebergflyway.MigrateMain"),
    // Hadoop registers JVM shutdown hooks that break inside sbt's in-process classloader.
    Compile / run / fork := true,
    // Fork tests so Test / envVars reaches Testcontainers.
    Test / fork := true,
    // Testcontainers would pull its Ryuk image on first use; the suites stop their containers
    // themselves, so the reaper is off.
    Test / envVars += "TESTCONTAINERS_RYUK_DISABLED" -> "true",
    // Hadoop's native code loader; applies to the forked `run` and tests.
    javaOptions += "--enable-native-access=ALL-UNNAMED",
    // Docker image for the ECS Fargate one-off task: `sbt Docker/publishLocal`, then
    // `docker run --rm -e FLYWAY_URL=... iceberg-flyway:<version> migrate`.
    dockerBaseImage      := "eclipse-temurin:21-jre",
    Docker / packageName := "iceberg-flyway"
  )
