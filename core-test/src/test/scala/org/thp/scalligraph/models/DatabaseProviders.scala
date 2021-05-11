package org.thp.scalligraph.models

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.thp.scalligraph.janus.JanusDatabase
import play.api.{Configuration, Environment, Logger}

import javax.inject.Provider
//import org.thp.scalligraph.neo4j.Neo4jDatabase
//import org.thp.scalligraph.orientdb.OrientDatabase

class DatabaseProviders(config: Configuration, system: ActorSystem) {

  def this(system: ActorSystem) =
    this(
      Configuration(ConfigFactory.parseString(s"db.janusgraph.storage.directory = target/janusgraph-test-database-${math.random()}.db")) withFallback
        Configuration.load(Environment.simple()),
      system
    )

  def this() = this(ActorSystem("DatabaseProviders"))

  lazy val logger: Logger = Logger(getClass)

  lazy val janus: DatabaseProvider = new DatabaseProvider("janus", new JanusDatabase(config, system, fullTextIndexAvailable = false))

//  lazy val orientdb: DatabaseProvider = new DatabaseProvider("orientdb", new OrientDatabase(config, system))
//
//  lazy val neo4j: DatabaseProvider = new DatabaseProvider("neo4j", new Neo4jDatabase(config, system))

  lazy val list: Seq[DatabaseProvider] = janus /* :: orientdb :: neo4j*/ :: Nil
}

class DatabaseProvider(val name: String, db: => Database) extends Provider[Database] {
  override lazy val get: Database = db
}
