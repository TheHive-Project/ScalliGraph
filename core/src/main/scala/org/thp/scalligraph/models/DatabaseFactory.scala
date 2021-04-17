package org.thp.scalligraph.models

import org.thp.scalligraph.ScalligraphApplication

import javax.inject.Provider

object DatabaseFactory {
  def apply(scalligraphApplication: ScalligraphApplication): Database = {
    val providerName = scalligraphApplication.configuration.get[String]("db.provider") match {
      case "janusgraph" => "org.thp.scalligraph.janus.JanusDatabaseProvider"
      case other        => other
    }
    getClass
      .getClassLoader
      .loadClass(providerName)
      .getConstructor(classOf[ScalligraphApplication])
      .newInstance(scalligraphApplication)
      .asInstanceOf[Provider[Database]]
      .get
  }
}
