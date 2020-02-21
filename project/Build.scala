import sbt._
import sbt.Keys._

object Build extends sbt.Build {

  lazy val root = Project("khipu", file("."))
    .aggregate(khipu_base, khipu_storage, khipu_eth, khipu_lmdb, khipu_kesque, khipu_bdb, khipu_rocksdb)
    .settings(basicSettings: _*)
    .settings(Formatting.buildFileSettings: _*)
    .settings(noPublishing: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka)
    .settings(Packaging.settings)
    .settings(
      mainClass in Compile := Some("khipu.Khipu")
    )

  lazy val khipu_base = Project("khipu-base", file("khipu-base"))
    .settings(basicSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka ++ Dependencies.spongycastle ++ Dependencies.scrypto ++ Dependencies.bouncycastle)
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

  lazy val khipu_storage = Project("khipu-storage", file("khipu-storage"))
    .dependsOn(khipu_base)
    .settings(basicSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka)
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

  lazy val khipu_kesque = Project("khipu-kesque", file("khipu-kesque"))
    .dependsOn(khipu_base)
    .dependsOn(khipu_storage)
    .dependsOn(khipu_lmdb)
    .dependsOn(khipu_rocksdb)
    .settings(basicSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.kafka ++ Dependencies.spongycastle ++ Dependencies.caffeine)
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

  lazy val khipu_bdb = Project("khipu-bdb", file("khipu-bdb"))
    .dependsOn(khipu_base)
    .dependsOn(khipu_storage)
    .settings(basicSettings: _*)
    .settings(noPublishing: _*)
    .settings(unmanagedJars in Compile ++= Seq(baseDirectory.value / "lib" / "db-5.3.28.jar").classpath)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka)
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

  lazy val khipu_rocksdb = Project("khipu-rocksdb", file("khipu-rocksdb"))
    .dependsOn(khipu_base)
    .dependsOn(khipu_storage)
    .settings(basicSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka ++ Dependencies.rocksdb)
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

  lazy val khipu_lmdb = Project("khipu-lmdb", file("khipu-lmdb"))
    .dependsOn(khipu_base)
    .dependsOn(khipu_storage)
    .settings(basicSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka ++ Dependencies.lmdb)
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

  lazy val khipu_eth = Project("khipu-eth", file("khipu-eth"))
    .dependsOn(khipu_base)
    .dependsOn(khipu_storage)
    .dependsOn(khipu_lmdb)
    .dependsOn(khipu_kesque)
    .settings(basicSettings: _*)
    .settings(noPublishing: _*)
    .settings(libraryDependencies ++= Dependencies.basic ++ Dependencies.akka ++ Dependencies.akka_http ++ Dependencies.others ++ Dependencies.spongycastle ++ Dependencies.scrypto ++ Dependencies.snappy ++ Dependencies.caffeine)
    .settings(libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(Packaging.settings)
    .settings(
      mainClass in Compile := Some("khipu.Khipu")
    )

  lazy val basicSettings = Defaults.coreDefaultSettings ++ Seq(
    organization := "khipu.io",
    version := "0.4.1-beta",
    resolvers ++= Seq(
      "Local Maven" at Path.userHome.asURL + ".m2/repository",
      "Typesafe repo" at "http://repo.typesafe.com/typesafe/releases/",
      "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
    ),
    fork in run := true,
    fork in Test := true,
    parallelExecution in Test := false,
    scalaVersion := "2.12.10",
    scalacOptions ++= Seq("-unchecked", "-deprecation")
  //javacOptions ++= Seq("-source", "1.8", "-target", "1.8")  // TODO options cause javadoc fail
  ) ++ Environment.settings ++ Formatting.settings

  lazy val noPublishing = Seq(
    publish := (),
    publishLocal := (),
    publishTo := None
  )
}

object Dependencies {

  private val AKKA_VERSION = "2.6.3"
  private val AKKA_HTTP_VERSION = "10.1.11"
  private val SLF4J_VERSION = "1.7.24"
  private val CIRCE_VERSION = "0.7.0"

  val akka = Seq(
    "com.typesafe.akka" %% "akka-actor" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-remote" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-cluster-sharding" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-cluster-tools" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-persistence" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-stream" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-slf4j" % AKKA_VERSION,
    "com.typesafe.akka" %% "akka-testkit" % AKKA_VERSION % Test,
    "com.typesafe.akka" %% "akka-multi-node-testkit" % AKKA_VERSION % Test,
    "org.iq80.leveldb" % "leveldb" % "0.10",
    "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % Runtime
  )

  val akka_http = Seq(
    "com.typesafe.akka" %% "akka-http-core" % AKKA_HTTP_VERSION,
    "com.typesafe.akka" %% "akka-http" % AKKA_HTTP_VERSION,
    "com.typesafe.akka" %% "akka-http-spray-json" % AKKA_HTTP_VERSION
  )

  val akka_management = Seq(
    "com.lightbend.akka.management" %% "akka-management" % "1.0.4",
    "com.lightbend.akka.management" %% "akka-management-cluster-http" % "1.0.4"
  )


  val lmdb = Seq("org.lmdbjava" % "lmdbjava" % "0.7.0") // akka-distributed-data also includes this lib

  val rocksdb = Seq("org.rocksdb" % "rocksdbjni" % "6.2.2")

  val kafka = Seq(
    "org.apache.kafka" % "kafka-clients" % "2.0.0",
    "org.apache.kafka" %% "kafka" % "2.0.0"
  )

  val bouncycastle = Seq("org.bouncycastle" % "bcprov-jdk15on" % "1.62")

  val spongycastle = Seq("com.madgag.spongycastle" % "core" % "1.58.0.0")

  val scrypto = Seq("org.consensusresearch" %% "scrypto" % "1.2.0-RC3")

  val snappy = Seq("org.xerial.snappy" % "snappy-java" % "1.1.7")

  val caffeine = Seq("com.github.ben-manes.caffeine" % "caffeine" % "2.6.2")

  val others = Seq(
    "ch.megard" %% "akka-http-cors" % "0.2.1",
    "org.json4s" %% "json4s-native" % "3.5.1",
    "de.heikoseeberger" %% "akka-http-json4s" % "1.11.0",
    "org.jline" % "jline" % "3.1.2",
    "commons-io" % "commons-io" % "2.5",
    "com.google.code.findbugs" % "jsr305" % "3.0.2" % Provided
  )


  val log = Seq(
    "org.slf4j" % "slf4j-api" % SLF4J_VERSION,
    "org.slf4j" % "jcl-over-slf4j" % SLF4J_VERSION,
    "org.slf4j" % "log4j-over-slf4j" % SLF4J_VERSION,
    "ch.qos.logback" % "logback-classic" % "1.2.1"
  )

  val test = Seq(
    "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % Test,
    "org.scalatest" %% "scalatest" % "3.0.1" % Test
  )

  val cassandra_driver = Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.2.0",
    "com.datastax.cassandra" % "cassandra-driver-extras" % "3.2.0",
    "org.xerial.snappy" % "snappy-java" % "1.1.4"
  )

  val basic: Seq[ModuleID] = log ++ test
}

object Environment {
  object BuildEnv extends Enumeration {
    val Production, Stage, Test, Developement = Value
  }
  val buildEnv = settingKey[BuildEnv.Value]("The current build environment")

  val settings = Seq(
    buildEnv := {
      sys.props.get("env")
        .orElse(sys.env.get("BUILD_ENV"))
        .flatMap {
          case "prod"  => Some(BuildEnv.Production)
          case "stage" => Some(BuildEnv.Stage)
          case "test"  => Some(BuildEnv.Test)
          case "dev"   => Some(BuildEnv.Developement)
          case _       => None
        }
        .getOrElse(BuildEnv.Developement)
    },
    onLoadMessage := {
      // old message as well
      val defaultMessage = onLoadMessage.value
      val env = buildEnv.value
      s"""|$defaultMessage
          |Working in build environment: $env""".stripMargin
    }
  )
}

object Formatting {
  import com.typesafe.sbt.SbtScalariform
  import com.typesafe.sbt.SbtScalariform.ScalariformKeys
  import ScalariformKeys._

  val BuildConfig = config("build") extend Compile
  val BuildSbtConfig = config("buildsbt") extend Compile

  // invoke: build:scalariformFormat
  val buildFileSettings: Seq[Setting[_]] = SbtScalariform.noConfigScalariformSettings ++
    inConfig(BuildConfig)(SbtScalariform.configScalariformSettings) ++
    inConfig(BuildSbtConfig)(SbtScalariform.configScalariformSettings) ++ Seq(
      scalaSource in BuildConfig := baseDirectory.value / "project",
      scalaSource in BuildSbtConfig := baseDirectory.value,
      includeFilter in (BuildConfig, format) := ("*.scala": FileFilter),
      includeFilter in (BuildSbtConfig, format) := ("*.sbt": FileFilter),
      format in BuildConfig := {
        val x = (format in BuildSbtConfig).value
        (format in BuildConfig).value
      },
      ScalariformKeys.preferences in BuildConfig := formattingPreferences,
      ScalariformKeys.preferences in BuildSbtConfig := formattingPreferences
    )

  val settings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test := formattingPreferences
  )

  val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, false)
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(SpacesAroundMultiImports, true)
      .setPreference(IndentSpaces, 2)
  }
}

object Packaging {
  // Good example https://github.com/typesafehub/activator/blob/master/project/Packaging.scala
  import com.typesafe.sbt.SbtNativePackager._
  import com.typesafe.sbt.packager.Keys._
  import com.typesafe.sbt.packager.archetypes._

  // This is dirty, but play has stolen our keys, and we must mimc them here.
  val stage = TaskKey[File]("stage")
  val dist = TaskKey[File]("dist")

  import Environment.{ BuildEnv, buildEnv }
  val settings = packageArchetype.java_application ++ Seq(
    name in Universal := s"${name.value}",
    dist <<= packageBin in Universal,
    mappings in Universal += {
      val confFile = buildEnv.value match {
        case BuildEnv.Developement => "dev.conf"
        case BuildEnv.Test         => "test.conf"
        case BuildEnv.Stage        => "stage.conf"
        case BuildEnv.Production   => "prod.conf"
      }
      (sourceDirectory(_ / "universal" / "conf").value / confFile) -> "conf/application.conf"
    }
  )
}

