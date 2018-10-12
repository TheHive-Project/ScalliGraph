package org.thp.scalligraph.query

import scala.reflect.runtime.{universe ⇒ ru}
import gremlin.scala.{Element, GremlinScala}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.{Mapping, ScalliSteps, UniMapping}

class PublicPropertyListBuilder[S <: ScalliSteps[_, E, S]: ru.TypeTag, E <: Element](propertyList: List[PublicProperty[E, _]]) {
  propertyListBuilder ⇒

  class PublicPropertyBuilder[D, G] private[PublicPropertyListBuilder] (propertyName: String, mapping: UniMapping[D]) {
    def simple: PublicPropertyListBuilder[S, E] =
      new PublicPropertyListBuilder[S, E](
        PublicProperty[E, G](ru.typeOf[S], propertyName, mapping.asInstanceOf[Mapping[_, _, G]], _ ⇒ Seq(_.value[G](propertyName))) :: propertyList)

    def derived(fn: Option[AuthContext] ⇒ GremlinScala[E] ⇒ GremlinScala[G]): PublicPropertyListBuilder[S, E] =
      new PublicPropertyListBuilder[S, E](
        PublicProperty[E, G](ru.typeOf[S], propertyName, mapping.asInstanceOf[Mapping[_, _, G]], fn.andThen(Seq(_))) :: propertyList)

    def seq(fn: Option[AuthContext] ⇒ Seq[GremlinScala[E] ⇒ GremlinScala[G]]): PublicPropertyListBuilder[S, E] =
      new PublicPropertyListBuilder[S, E](
        PublicProperty[E, G](ru.typeOf[S], propertyName, mapping.asInstanceOf[Mapping[_, _, G]], fn) :: propertyList)
  }

  def property[D](propertyName: String)(implicit mapping: UniMapping[D]) = new PublicPropertyBuilder[D, mapping.GraphType](propertyName, mapping)

  def build: List[PublicProperty[E, _]] = propertyList
}

object PublicPropertyListBuilder {
  def apply[S <: ScalliSteps[_, E, S]: ru.TypeTag, E <: Element]: PublicPropertyListBuilder[S, E] = new PublicPropertyListBuilder[S, E](Nil)
}

case class PublicProperty[E <: Element, G](
    stepType: ru.Type,
    propertyName: String,
    mapping: Mapping[_, _, G],
    fn: Option[AuthContext] ⇒ Seq[GremlinScala[E] ⇒ GremlinScala[G]])
