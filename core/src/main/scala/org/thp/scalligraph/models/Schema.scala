package org.thp.scalligraph.models

import javax.inject.{Inject, Named, Singleton}
import org.thp.scalligraph.auth.UserSrv
import play.api.Logger

case class InitialValue[V <: Product](model: Model.Vertex[V], value: V)

trait Schema {
  def modelList: Seq[Model]
  def initialValues: Seq[InitialValue[_]]
  def getModel(label: String): Option[Model] = modelList.find(_.label == label)
}

@Singleton
class SchemaChecker @Inject()(@Named("schemaVersion") version: Int, schema: Schema, db: Database, userSrv: UserSrv) {
  if (db.version < version) {
    Logger(getClass).info(s"Database schema version is outdated (${db.version}). Upgrading to $version ...")
    db.createSchemaFrom(schema)(userSrv.initialAuthContext)
    db.setVersion(version)
  }
}
