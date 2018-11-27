package org.thp.scalligraph.models

import javax.inject.{Inject, Provider}
import org.thp.scalligraph.janus.JanusDatabase
import org.thp.scalligraph.neo4j.Neo4jDatabase
import org.thp.scalligraph.orientdb.OrientDatabase
import play.api.{Configuration, Logger}

class DatabaseProviders @Inject()(config: Configuration) {

  def this() = this(Configuration.empty)

  lazy val logger = Logger(getClass)

  lazy val janus: DatabaseProvider = new DatabaseProvider("janus", new JanusDatabase(config))

  lazy val orientdb: DatabaseProvider = new DatabaseProvider("orientdb", new OrientDatabase(config))

  lazy val neo4j: DatabaseProvider = new DatabaseProvider("neo4j", new Neo4jDatabase(config))

  lazy val list: Seq[DatabaseProvider] = janus :: orientdb :: neo4j :: Nil
}

class DatabaseProvider(val name: String, db: ⇒ Database) extends Provider[Database] {
  private lazy val _db = db

  override def get(): Database = _db
}
