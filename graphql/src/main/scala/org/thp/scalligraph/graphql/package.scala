package org.thp.scalligraph

import java.util.Date

import sangria.schema.{LongType, ScalarAlias}

package object graphql {
  val DateType: ScalarAlias[Date, Long] = ScalarAlias[Date, Long](LongType, _.getTime, ts ⇒ Right(new Date(ts)))
}
