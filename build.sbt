import Dependencies._

lazy val scalligraph = (project in file("."))
  .dependsOn(core, common, ctrl, graphql, janus, orientdb, neo4j, coreTest)
  .dependsOn(coreTest % "test -> test")
  .aggregate(core, common, ctrl, graphql, janus, orientdb, neo4j, coreTest)
  .settings(
    inThisBuild(
      List(
        organization := "org.thp",
        scalaVersion := "2.12.6",
        resolvers += Resolver.mavenLocal,
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
          //"-Ylog-classpath",
          //"-Xlog-implicits",
          //"-Yshow-trees-compact",
          //"-Yshow-trees-stringified",
          //"-Ymacro-debug-lite",
          "-Xlog-free-types",
          "-Xlog-free-terms",
          "-Xprint-types"
        ),
        fork in Test := true,
        javaOptions += "-Xmx1G",
        addCompilerPlugin(macroParadise),
        scalafmtConfig := Some(file(".scalafmt.conf"))
      )),
    name := "scalligraph"
  )

lazy val core = (project in file("core"))
  .dependsOn(ctrl)
  .settings(
    name := "scalligraph-core",
    libraryDependencies ++= Seq(
      gremlinScala,
      scalactic,
      scalaCompiler(scalaVersion.value),
      scalaGuice,
      specs % Test
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
    name := "scalligraph-janus",
    libraryDependencies ++= Seq(
      janusGraph,
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

lazy val common = (project in file("common"))
  .settings(
    name := "scalligraph-common",
    libraryDependencies ++= Seq(
      scalaReflect(scalaVersion.value),
      playCore,
      scalactic,
      apacheConfiguration,
      specs % Test
    )
  )

lazy val ctrl = (project in file("ctrl"))
  .dependsOn(common)
  .settings(
    name := "scalligraph-ctrl",
    libraryDependencies ++= Seq(
      scalaReflect(scalaVersion.value),
      scalaCompiler(scalaVersion.value),
      playCore,
      scalaGuice,
      bouncyCastle,
      shapeless,
      specs % Test
    )
  )

lazy val graphql = (project in file("graphql"))
  .dependsOn(core)
  .dependsOn(coreTest % "test->test")
  .dependsOn(common)
  .dependsOn(janus)
  .settings(
    name := "scalligraph-graphql",
    libraryDependencies ++= Seq(
      scalaGuice,
      sangria,
      sangriaPlay,
      specs % Test
    )
  )
