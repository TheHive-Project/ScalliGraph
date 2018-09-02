package org.thp.scalligraph.models

import gremlin.scala.{Element, GremlinScala}

abstract class EntityFilter[G <: Element] {
  def apply(raw: GremlinScala[G]): GremlinScala[G]
}

object EntityFilter {
  def apply[G <: Element](f: GremlinScala[G] ⇒ GremlinScala[G]): EntityFilter[G] =
    (raw: GremlinScala[G]) ⇒ f(raw)

  def noFilter[G <: Element]: EntityFilter[G] = (raw: GremlinScala[G]) ⇒ raw

  def or[G <: Element](filters: List[EntityFilter[G]]): EntityFilter[G] =
    (raw: GremlinScala[G]) ⇒
      filters match {
        case Nil      ⇒ raw
        case f :: Nil ⇒ f(raw)
        case fs       ⇒ raw.or(fs.map(x ⇒ x.apply _): _*)
    }

  def build[G <: Element](input: Map[String, Option[Any]], fields: Map[String, Function[Any, EntityFilter[G]]]): EntityFilter[G] = {
    val entityFilters = input.collect {
      case (key, Some(value)) ⇒ fields(key)(value)
    }
    or(entityFilters.toList)
  }
}
