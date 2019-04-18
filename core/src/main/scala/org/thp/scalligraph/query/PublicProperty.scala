package org.thp.scalligraph.query

import java.util.Date

import scala.reflect.runtime.{universe ⇒ ru}

import play.api.libs.json.Writes

import gremlin.scala.{Element, GremlinScala}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.{Database, Mapping, ScalliSteps, UniMapping}

//object ReadonlyProperty {
//  def apply[E, D](): (GremlinScala[E], Database, AuthContext, D) ⇒ Unit =
//    (elementTraversal: GremlinScala[E], db: Database, authContext: AuthContext, value: D) ⇒
//    sys.error("readonly property") // FIXME
//}
class PublicPropertyListBuilder[S <: ScalliSteps[_, E, S]: ru.TypeTag, E <: Element](propertyList: List[PublicProperty[E, _, _]]) {
  propertyListBuilder ⇒

  class PublicPropertySeqBuilder[D, G] private[PublicPropertyListBuilder] (
      val fnList: AuthContext ⇒ Seq[(GremlinScala[E] ⇒ GremlinScala[_ <: Element], String)] = _ ⇒ Nil) {

    def derived(fn: AuthContext ⇒ GremlinScala[E] ⇒ GremlinScala[_ <: Element], graphPropertyName: String): PublicPropertySeqBuilder[D, G] =
      new PublicPropertySeqBuilder[D, G]((authContext: AuthContext) ⇒ fnList(authContext) :+ (fn(authContext) → graphPropertyName))
    def simple(graphPropertyName: String): PublicPropertySeqBuilder[D, G] = derived(_ ⇒ identity, graphPropertyName)
  }

  class PublicPropertyBuilder[D: Writes, G] private[PublicPropertyListBuilder] (propertyName: String, mapping: UniMapping[D]) {
    def simple: PublicPropertyListBuilder[S, E] = {
      val definition = (_: GremlinScala[E]).value[G](propertyName)
      new PublicPropertyListBuilder(
        new PublicProperty[E, D, G](
          ru.typeOf[S],
          propertyName,
          mapping.asInstanceOf[Mapping[D, _, G]],
//      Seq(_.value[G](propertyName))
          Seq(definition)
        ) {
          override def update(elementTraversal: GremlinScala[E], db: Database, authContext: AuthContext, value: D): Unit =
            elementTraversal.headOption().foreach { element ⇒
              db.setOptionProperty(element, "_updatedAt", Some(new Date), db.updatedAtMapping)
              db.setOptionProperty(element, "_updatedBy", Some(authContext.userId), db.updatedByMapping)
              db.setProperty(element, propertyName, value, mapping)
            }
        } :: propertyList)
    }

    def rename(graphPropertyName: String): PublicPropertyListBuilder[S, E] = {
      val definition = (_: GremlinScala[E]).value[G](propertyName)
      new PublicPropertyListBuilder(
        new PublicProperty[E, D, G](
          ru.typeOf[S],
          propertyName,
          mapping.asInstanceOf[Mapping[D, _, G]],
          Seq(definition)
        ) {
          override def update(elementTraversal: GremlinScala[E], db: Database, authContext: AuthContext, value: D): Unit =
            elementTraversal.headOption().foreach { element ⇒
              db.setOptionProperty(element, "_updatedAt", Some(new Date), db.updatedAtMapping)
              db.setOptionProperty(element, "_updatedBy", Some(authContext.userId), db.updatedByMapping)
              db.setProperty(element, graphPropertyName, value, mapping)
            }
        } :: propertyList)
    }

    def derived(definition: (GremlinScala[E] ⇒ GremlinScala[G])*)(
        updateFunction: (GremlinScala[E], Database, AuthContext, D) ⇒ Unit): PublicPropertyListBuilder[S, E] =
      new PublicPropertyListBuilder(
        new PublicProperty[E, D, G](
          ru.typeOf[S],
          propertyName,
          mapping.asInstanceOf[Mapping[D, _, G]],
          definition
        ) {
          override def update(elementTraversal: GremlinScala[E], db: Database, authContext: AuthContext, value: D): Unit =
            updateFunction(elementTraversal, db, authContext, value)
        } :: propertyList)

//    def seq(f: PublicPropertySeqBuilder[D, G] ⇒ PublicPropertySeqBuilder[D, G]): PublicPropertyListBuilder[S, E] = ???
////      new PublicPropertyListBuilder[S, E](PublicProperty[E, D, G](
////        ru.typeOf[S],
////        propertyName,
////        mapping.asInstanceOf[Mapping[_, D, G]],
////        f(new PublicPropertySeqBuilder[D, G]).fnList,
////        (a, b, c, d) => ()) :: propertyList) // FIXME
  }

  def property[D: Writes](propertyName: String)(implicit mapping: UniMapping[D]) =
    new PublicPropertyBuilder[D, mapping.GraphType](propertyName, mapping)

  def build: List[PublicProperty[E, _, _]] = propertyList
}

object PublicPropertyListBuilder {
  def apply[S <: ScalliSteps[_, E, S]: ru.TypeTag, E <: Element]: PublicPropertyListBuilder[S, E] = new PublicPropertyListBuilder[S, E](Nil)
}

object PublicProperty {
  def readonly[E, D]: (GremlinScala[E], Database, AuthContext, D) ⇒ Unit =
    (elementTraversal: GremlinScala[E], db: Database, authContext: AuthContext, value: D) ⇒ sys.error("readonly property") // FIXME
}

abstract class PublicProperty[E <: Element, D: Writes, G](
    val stepType: ru.Type,
    val propertyName: String,
    val mapping: Mapping[D, _, G],
    val definition: Seq[GremlinScala[E] ⇒ GremlinScala[G]]) {
//
//},
//    getFn: PropertyContext => GremlinScala[G],
//    updateFn: (PropertyContext, Any) => ()) {

  def update(elementTraversal: GremlinScala[E], db: Database, authContext: AuthContext, value: D): Unit
//    updateFn(new PropertyContext(db, authContext, elementTraversal))
//    (elementFunc, prop) ← property.fn(Some(authContext)).headOption.asInstanceOf[Option[(GremlinScala[Any] ⇒ GremlinScala[Any], String)]]
//    e = elementSrv.get(id)(graph).asInstanceOf[ElementSteps[_, _, _]].raw.asInstanceOf[GremlinScala[Any]]
//    db.setOptionProperty(element, "_updatedAt", Some(new Date), db.updatedAtMapping)
//    db.setOptionProperty(element, "_updatedBy", Some(authContext.userId), db.updatedByMapping)

//    elementTraversal.clone.traversal.limit(1).toList.asScala.headOption.asInstanceOf[Option[Element]].foreach { element ⇒
  //logger.trace(s"Update ${element.id()} by ${authContext.userId}: $key = $value")
//      updateFunc(db, authContext, elementTraversal, value)
//  }
//  }
  def get(elementTraversal: GremlinScala[E], authContext: AuthContext): GremlinScala[G] =
    if (definition.lengthCompare(1) == 0)
      definition.head.apply(elementTraversal)
    else
      elementTraversal.coalesce(definition: _*)
  //  def get(authContext: AuthContext): Seq[GremlinScala[E] ⇒ GremlinScala[G]] = fn(authContext).map {
//    case (f, p) ⇒ f.andThen(_.value[G](p))
//  }

//  def apply[T](from: GremlinScala[T], authContext: AuthContext): GremlinScala[G] = ??? // TODO
//    val props = get(authContext).asInstanceOf[Seq[GremlinScala[T] ⇒ GremlinScala[G]]]
//    if (props.size == 1)
//      props.head(from)
//    else
//      from.coalesce(props: _*)
//  }

//  lazy val jsonWrites: Writes[G] = Writes[G](g ⇒ ???) //Json.toJson(mapping.toDomain(g))) FIXME
}
