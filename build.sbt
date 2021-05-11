import Dependencies._

val defaultSettings = Seq(
  version := "0.1.0-SNAPSHOT",
  organization := "org.thp",
  scalaVersion := "2.13.5",
  resolvers ++= Seq(
    Resolver.mavenLocal,
    "Oracle Released Java Packages" at "https://download.oracle.com/maven",
    "TheHive project repository" at "https://dl.bintray.com/thehive-project/maven/"
  ),
  crossScalaVersions := Nil,
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-feature",     // Emit warning and location for usages of features that should be imported explicitly.
    "-unchecked",   // Enable additional warnings where generated code depends on assumptions.
    //"-Xfatal-warnings",      // Fail the compilation if there are any warnings.
    "-Xlint", // Enable recommended additional warnings.
    //    "-Ywarn-adapted-args",     // Warn if an argument list is modified to match the receiver.
    //"-Ywarn-dead-code",      // Warn when dead code is identified.
    //    "-Ywarn-inaccessible",     // Warn about inaccessible types in method signatures.
    //    "-Ywarn-nullary-override", // Warn when non-nullary overrides nullary, e.g. def foo() over def foo.
    "-Ywarn-numeric-widen", // Warn when numerics are widened.
    "-Ywarn-value-discard", // Warn when non-Unit expression results are unused
    //"-Ylog-classpath",
    //"-Xlog-implicits",
    //"-Yshow-trees-compact",
    //"-Yshow-trees-stringified",
    //"-Ymacro-debug-lite",
    "-Xlog-free-types",
    "-Xlog-free-terms",
    "-Xprint-types",
    "-Ymacro-annotations"
  ),
  scalafmtConfig := (ThisBuild / baseDirectory).value / ".scalafmt.conf",
  Test / fork := true,
  dependencyOverrides += "io.netty" % "netty-all" % "4.0.56.Final",
  Compile / packageDoc / publishArtifact := false,
  Compile / doc / sources := Nil,
  Test / packageDoc / publishArtifact := false,
  Test / doc / sources := Nil
)

val noPackageSettings = Seq(
  Universal / stage := file(""),
  Universal / packageBin := file(""),
  Debian / stage := file(""),
  Debian / packageBin := file(""),
  Rpm / stage := file(""),
  Rpm / packageBin := file(""),
  publish / skip := true
)

lazy val scalligraphRoot = (project in file("."))
//  .dependsOn(core, /*graphql, */ janus /* , orientdb , neo4j, coreTest*/ )
//  .dependsOn(coreTest % "test -> test")
  .aggregate(scalligraph, /*graphql, */ scalligraphJanusgraph /* , orientdb, neo4j */, scalligraphTest)
  .settings(defaultSettings)
  .settings(noPackageSettings)
  .settings(
    name := "root"
  )

lazy val scalligraph = (project in file("core"))
  .settings(defaultSettings)
  .settings(noPackageSettings)
  .settings(
    name := "scalligraph",
    libraryDependencies ++= Seq(
      tinkerpop,
      scalactic,
      hadoopClient,
      alpakkaS3,
      akkaHttp,
      akkaHttpXml,
      playCore,
      apacheConfiguration,
      bouncyCastle,
      shapeless,
      caffeine,
      akkaCluster,
      akkaClusterTools,
      akkaClusterTyped,
      specs       % Test,
      playLogback % Test,
      scalaCompiler(scalaVersion.value),
      scalaReflect(scalaVersion.value),
      ws,
      macWireMacros,
      macWireMacrosakka,
      macWireUtil,
      macWireProxy,
      refined
    )
  )

lazy val scalligraphTest = (project in file("core-test"))
  .dependsOn(scalligraph)
  .dependsOn(scalligraphJanusgraph)
  //  .dependsOn(orientdb)
  //  .dependsOn(neo4j)
  .settings(defaultSettings)
  .settings(noPackageSettings)
  .settings(
    name := "scalligraph-test",
    libraryDependencies ++= Seq(
      janusGraphInMemory % Test,
      specs              % Test,
      playLogback        % Test
    )
  )

lazy val scalligraphJanusgraph = (project in file("database/janusgraph"))
  .dependsOn(scalligraph)
  .settings(defaultSettings)
  .settings(noPackageSettings)
  .settings(
    name := "scalligraph-janusgraph",
    libraryDependencies ++= Seq(
      janusGraph,
      janusGraphBerkeleyDB,
//      janusGraphHBase,
      janusGraphLucene,
      janusGraphElasticSearch,
      janusGraphCassandra,
//      janusGraphDriver,
//      janusGraphCore,
      specs % Test
    )
  )

//lazy val orientdb = (project in file("database/orientdb"))
//  .dependsOn(core)
//  .settings(
//    name := "scalligraph-orientdb",
//    version := scalligraphVersion,
//    libraryDependencies ++= Seq(
//      gremlinScala,
//      gremlinOrientdb,
//      specs % Test
//    )
//  )
//
//lazy val neo4j = (project in file("database/neo4j"))
//  .dependsOn(core)
//  .settings(
//    name := "scalligraph-neo4j",
//    version := scalligraphVersion,
//    libraryDependencies ++= Seq(
//      gremlinScala,
//      neo4jGremlin,
//      neo4jTinkerpop,
//      specs % Test
//    )
//  )

//lazy val graphql = (project in file("graphql"))
//  .dependsOn(core)
//  .dependsOn(coreTest % "test->test")
//  .dependsOn(janus)
//  .settings(
//    name := "scalligraph-graphql",
//    version := scalligraphVersion,
//    libraryDependencies ++= Seq(
//      scalaGuice,
//      sangria,
//      sangriaPlay,
//      specs % Test
//    )
//  )
