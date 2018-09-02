package org.thp.scalligraph

import java.util.Date

import _root_.sangria.schema.{ListType, LongType, OptionType, OutputType, ScalarAlias}

package object graphql {
  implicit val DateType: ScalarAlias[Date, Long] =
    ScalarAlias[Date, Long](LongType, _.getTime, ts ⇒ Right(new Date(ts)))
  implicit def optionType[A](implicit ot: OutputType[A]): OutputType[Option[A]] = OptionType(ot)
  implicit def listType[A](implicit lt: OutputType[A]): OutputType[Seq[A]]      = ListType(lt)
}
