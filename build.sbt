import Dependencies._

lazy val scala212               = "2.12.8"
lazy val scala213               = "2.13.0"
lazy val supportedScalaVersions = List(scala212) //, scala213)

// format: off
lazy val scalligraph = (project in file("."))
  .dependsOn(core, /*graphql, */janus, orientdb/*, neo4j, coreTest*/)
  .dependsOn(coreTest % "test -> test")
  .aggregate(core, /*graphql, */janus, orientdb, neo4j, coreTest)
  .settings(
    inThisBuild(
      List(
        organization := "org.thp",
        scalaVersion := "2.12.8",
        crossScalaVersions := supportedScalaVersions,
        resolvers ++= Seq(
          Resolver.mavenLocal,
          "Oracle Released Java Packages" at "http://download.oracle.com/maven",
          "TheHive project repository" at "https://dl.bintray.com/thehive-project/maven/"
        ),
        scalacOptions ++= Seq(
          "-encoding",
          "UTF-8",
          "-deprecation",            // Emit warning and location for usages of deprecated APIs.
          "-feature",                // Emit warning and location for usages of features that should be imported explicitly.
          "-unchecked",              // Enable additional warnings where generated code depends on assumptions.
          //"-Xfatal-warnings",      // Fail the compilation if there are any warnings.
          "-Xlint",                  // Enable recommended additional warnings.
          "-Ywarn-adapted-args",     // Warn if an argument list is modified to match the receiver.
          //"-Ywarn-dead-code",      // Warn when dead code is identified.
          "-Ywarn-inaccessible",     // Warn about inaccessible types in method signatures.
          "-Ywarn-nullary-override", // Warn when non-nullary overrides nullary, e.g. def foo() over def foo.
          "-Ywarn-numeric-widen",    // Warn when numerics are widened.
          "-Ywarn-value-discard",    // Warn when non-Unit expression results are unused
          "-Ywarn-unused:_,-explicits,-implicits",
          //"-Ylog-classpath",
          //"-Xlog-implicits",
          //"-Yshow-trees-compact",
          //"-Yshow-trees-stringified",
          //"-Ymacro-debug-lite",
          "-Xlog-free-types",
          "-Xlog-free-terms",
          "-Xprint-types"
        ),
        scalacOptions ++= {
          CrossVersion.partialVersion(scalaVersion.value) match {
            case Some((2, n)) if n >= 13 ⇒ "-Ymacro-annotations" :: Nil
            case _ ⇒ Nil
          }
        },
        libraryDependencies ++= {
          CrossVersion.partialVersion(scalaVersion.value) match {
            case Some((2, n)) if n >= 13 ⇒ Nil
            case _ ⇒ compilerPlugin(macroParadise) :: Nil
          }
        },
        fork in Test := true,
        javaOptions += "-Xmx1G",
//        addCompilerPlugin(macroParadise),
        scalafmtConfig := file(".scalafmt.conf")
      )),
    crossScalaVersions := Nil,
    name := "scalligraph"
  )
// format: on

lazy val core = (project in file("core"))
  .settings(
    name := "scalligraph-core",
    libraryDependencies ++= Seq(
      gremlinScala,
      scalactic,
      playGuice,
      scalaGuice,
      hadoopClient,
      playCore,
      apacheConfiguration,
      reflections,
      bouncyCastle,
      shapeless,
      scalaCompiler(scalaVersion.value),
      scalaReflect(scalaVersion.value),
      specs       % Test,
      playLogback % Test
    )
  )

lazy val coreTest = (project in file("core-test"))
  .dependsOn(core)
  .dependsOn(janus)
  .dependsOn(orientdb)
  .dependsOn(neo4j)
  .settings(
    name := "scalligraph-core-test",
    libraryDependencies ++= Seq(
      specs       % Test,
      playLogback % Test
    )
  )

lazy val janus = (project in file("database/janusgraph"))
  .dependsOn(core)
  .settings(
    name := "scalligraph-janusgraph",
    libraryDependencies ++= Seq(
      janusGraph,
      janusGraphBerkeleyDB,
      janusGraphHBase,
//      hbaseClient,
      cassandra,
//      hbaseCommon,
//      hadoopCommon,
//      gremlinServer,
      specs % Test
    )
  )

lazy val orientdb = (project in file("database/orientdb"))
  .dependsOn(core)
  .settings(
    name := "scalligraph-orientdb",
    libraryDependencies ++= Seq(
      gremlinScala,
      gremlinOrientdb,
      specs % Test
    )
  )

lazy val neo4j = (project in file("database/neo4j"))
  .dependsOn(core)
  .settings(
    name := "scalligraph-neo4j",
    libraryDependencies ++= Seq(
      gremlinScala,
      neo4jGremlin,
      neo4jTinkerpop,
      specs % Test
    )
  )

//lazy val graphql = (project in file("graphql"))
//  .dependsOn(core)
//  .dependsOn(coreTest % "test->test")
//  .dependsOn(janus)
//  .settings(
//    name := "scalligraph-graphql",
//    libraryDependencies ++= Seq(
//      scalaGuice,
//      sangria,
//      sangriaPlay,
//      specs % Test
//    )
//  )
