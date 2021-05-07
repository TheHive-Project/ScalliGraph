package org.thp.scalligraph.models

import org.thp.scalligraph.ScalligraphApplication

import javax.inject.Provider

object DatabaseFactory {
  def apply(app: ScalligraphApplication): Database = {
    val providerName = app.configuration.get[String]("db.provider") match {
      case "janusgraph" => "org.thp.scalligraph.janus.JanusDatabaseProvider"
      case other        => other
    }
    getClass
      .getClassLoader
      .loadClass(providerName)
      .getConstructor(classOf[ScalligraphApplication])
      .newInstance(app)
      .asInstanceOf[Provider[Database]]
      .get
  }
}
