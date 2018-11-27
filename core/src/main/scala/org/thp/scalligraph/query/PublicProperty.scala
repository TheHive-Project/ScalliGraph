package org.thp.scalligraph.query

import scala.reflect.runtime.{universe ⇒ ru}

import gremlin.scala.{Element, GremlinScala}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.{Mapping, ScalliSteps, UniMapping}

class PublicPropertyListBuilder[S <: ScalliSteps[_, E, S]: ru.TypeTag, E <: Element](propertyList: List[PublicProperty[E, _]]) {
  propertyListBuilder ⇒

  class PublicPropertySeqBuilder[D, G] private[PublicPropertyListBuilder] (
      val fnList: Option[AuthContext] ⇒ Seq[(GremlinScala[E] ⇒ GremlinScala[_ <: Element], String)] = _ ⇒ Nil) {

    def derived(fn: Option[AuthContext] ⇒ GremlinScala[E] ⇒ GremlinScala[_ <: Element], graphPropertyName: String): PublicPropertySeqBuilder[D, G] =
      new PublicPropertySeqBuilder[D, G]((authContext: Option[AuthContext]) ⇒ fnList(authContext) :+ (fn(authContext) → graphPropertyName))
    def simple(graphPropertyName: String): PublicPropertySeqBuilder[D, G] = derived(_ ⇒ identity, graphPropertyName)
  }

  class PublicPropertyBuilder[D, G] private[PublicPropertyListBuilder] (propertyName: String, mapping: UniMapping[D]) {
    def simple: PublicPropertyListBuilder[S, E] = derived(_ ⇒ identity, propertyName)

    def rename(graphPropertyName: String): PublicPropertyListBuilder[S, E] = derived(_ ⇒ identity, graphPropertyName)

    def derived(fn: Option[AuthContext] ⇒ GremlinScala[E] ⇒ GremlinScala[_ <: Element], graphPropertyName: String): PublicPropertyListBuilder[S, E] =
      new PublicPropertyListBuilder[S, E](PublicProperty[E, G](
        ru.typeOf[S],
        propertyName,
        mapping.asInstanceOf[Mapping[_, _, G]],
        fn.andThen(g ⇒ Seq(g → graphPropertyName))) :: propertyList)

    def seq(f: PublicPropertySeqBuilder[D, G] ⇒ PublicPropertySeqBuilder[D, G]): PublicPropertyListBuilder[S, E] =
      new PublicPropertyListBuilder[S, E](PublicProperty[E, G](
        ru.typeOf[S],
        propertyName,
        mapping.asInstanceOf[Mapping[_, _, G]],
        f(new PublicPropertySeqBuilder[D, G]).fnList) :: propertyList)
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
    fn: Option[AuthContext] ⇒ Seq[(GremlinScala[E] ⇒ GremlinScala[_ <: Element], String)]) {
  def get(authContext: Option[AuthContext]): Seq[GremlinScala[E] ⇒ GremlinScala[G]] = fn(authContext).map {
    case (f, p) ⇒ f.andThen(_.value[G](p))
  }
}
