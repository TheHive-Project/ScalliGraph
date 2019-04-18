package org.thp.scalligraph.models

import play.api.Logger

import gremlin.scala.Graph
import javax.inject.{Inject, Named, Singleton}
import org.thp.scalligraph.auth.{AuthContext, UserSrv}

case class InitialValue[V <: Product](model: Model.Vertex[V], value: V) {
  def create()(implicit db: Database, graph: Graph, authContext: AuthContext): V with Entity =
    db.createVertex[V](graph, authContext, model, value)
}

trait Schema {
  def modelList: Seq[Model]
  def initialValues: Seq[InitialValue[_]]
  def getModel(label: String): Option[Model]                      = modelList.find(_.label == label)
  def init(implicit graph: Graph, authContext: AuthContext): Unit = ()
}

@Singleton
class SchemaChecker @Inject()(@Named("schemaVersion") version: Int, schema: Schema, db: Database, userSrv: UserSrv) {
  val currentVersion: Int = db.version
  if (currentVersion < version) {
    Logger(getClass).info(s"Database schema version is outdated ($currentVersion). Upgrading to $version ...")
    db.createSchemaFrom(schema)(userSrv.initialAuthContext)
    db.setVersion(version)
  }
}
