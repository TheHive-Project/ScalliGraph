package org.thp.scalligraph.models
import play.api.Logger

import javax.inject.{Inject, Named, Singleton}
import org.thp.scalligraph.auth.UserSrv

trait Schema

@Singleton
class SchemaChecker @Inject()(@Named("schemaVersion") version: Int, schema: Schema, db: Database, userSrv: UserSrv) {
  {
    val dbVersion = db.version
    if (dbVersion < version) {
      Logger(getClass).info(s"Database schema version is outdated ($dbVersion). Upgrading to $version ...")
      db.createSchemaFrom(schema)(userSrv.initialAuthContext)
      db.setVersion(version)
    }
  }
}
