package org.thp.scalligraph.graphql

import gremlin.scala.GremlinScala
import org.thp.scalligraph.models.ScalliSteps
import org.thp.scalligraph.query.{AuthGraph, QueryExecutor}
import sangria.schema.{Field, ListType, OptionType, OutputType, fields, Schema ⇒ SangriaSchema}

import scala.language.experimental.macros

object Schema {
  def apply[E <: QueryExecutor[_]](executor: E): SangriaSchema[AuthGraph, Unit] = macro SchemaMacro.buildSchema[E]

  def scalliStepsFields[S <: ScalliSteps[T, _, _], T](stepType: OutputType[S], subType: OutputType[T]): List[Field[AuthGraph, S]] =
    fields[AuthGraph, S](
      Field("toList", ListType(subType), resolve = _.value.toList),
      Field("head", subType, resolve = _.value.head),
      Field("headOption", OptionType(subType), resolve = _.value.headOption),
//      Field("sort", stepType, resolve = _.value.sort.asInstanceOf[S]),
    )

  def gremlinScalaFields[S <: GremlinScala[T], T](stepType: OutputType[S], subType: OutputType[T]): List[Field[AuthGraph, S]] =
    fields[AuthGraph, S](
      Field("toList", ListType(subType), resolve = _.value.toList),
      Field("head", subType, resolve = _.value.head),
      Field("headOption", OptionType(subType), resolve = _.value.headOption),
      Field("sort", stepType, resolve = _.value.order().asInstanceOf[S]),
    )
}
