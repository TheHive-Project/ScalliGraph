package org.thp.scalligraph.traversal

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.{Element, T}
import org.thp.scalligraph.EntityId
import org.thp.scalligraph.models.{Entity, Mapping, UMapping}
import org.thp.scalligraph.utils.FieldSelector

import java.util.{Date, NoSuchElementException}
import scala.language.implicitConversions

trait ElementTraversalOps { _: TraversalOps =>
  implicit def predicateProvider[A](value: A): P[A] = P.eq(value)

  implicit class ElementTraversalOpsDefs[D, G <: Element, C <: Converter[D, G]](val traversal: Traversal[D, G, C]) {

    def remove(): Unit = {
      traversal.debug("drop")
      traversal.raw.drop().iterate()
      ()
    }

    def entity: Traversal[Product with Entity, Element, Converter[Product with Entity, Element]] =
      traversal.onRawMap[Product with Entity, Element, Converter[Product with Entity, Element]](_.asInstanceOf[GraphTraversal[_, Element]]) {
        (element: Element) =>
          new Product with Entity {
            override def productElement(n: Int): Any = throw new NoSuchElementException(s"Product0.productElement($n)")

            override def productArity: Int = 0

            override val _id: EntityId              = EntityId(element.id())
            override val _label: String             = element.label()
            override val _createdBy: String         = UMapping.string.getProperty(element, "_createdBy")
            override val _updatedBy: Option[String] = UMapping.string.optional.getProperty(element, "_updatedBy")
            override val _createdAt: Date           = UMapping.date.getProperty(element, "_createdAt")
            override val _updatedAt: Option[Date]   = UMapping.date.optional.getProperty(element, "_createdAt")

            override def canEqual(that: Any): Boolean =
              that match {
                case entity: Entity => entity._id == _id
                case _              => false
              }
          }
      }

    def hasLabel(name: String): Traversal[D, G, C] = traversal.onRaw(_.hasLabel(name))

    def has(accessor: T, predicate: P[_]): Traversal[D, G, C] =
      traversal.onRaw(_.has(accessor, traversal.graph.db.mapPredicate(predicate)))

//    def has[A, B](fieldSelect: D => FieldSelector[D, A], value: B)(implicit mapping: Mapping[A, B, _], ev: G <:< Element): Traversal[D, G, C] =
//      traversal.onRaw(_.has(fieldSelect(null.asInstanceOf[D]).name, mapping.reverse(value)))
    //    def has[A, B](fieldSelect: D => FieldSelector[D, A], predicate: P[B])(implicit mapping: Mapping[A, B, _], ev: G <:< Element): Traversal[D, G, C] =
    //      traversal.onRaw(_.has(fieldSelect(null.asInstanceOf[D]).name, traversal.graph.db.mapPredicate(predicate)))
    def has[A, B](fieldSelect: D => FieldSelector[D, A], predicate: P[B])(implicit mapping: Mapping[A, B, _]): Traversal[D, G, C] =
      traversal.onRaw(_.has(fieldSelect(null.asInstanceOf[D]).name, traversal.graph.db.mapPredicate(mapping.reverse(predicate))))
    def has[A](fieldSelect: D => FieldSelector[D, A]): Traversal[D, G, C] =
      traversal.onRaw(_.has(fieldSelect(null.asInstanceOf[D]).name))
//    def hasNot[A, B](fieldSelect: D => FieldSelector[D, A], predicate: P[B])(implicit mapping: Mapping[A, B, _]): Traversal[D, G, C] =
//      traversal.onRaw(_.has(fieldSelect(null.asInstanceOf[D]).name, traversal.graph.db.mapPredicate(mapping.reverse(predicate.negate()))))
//    def hasNot[A, B](fieldSelect: D => FieldSelector[D, A], value: B)(implicit mapping: Mapping[A, B, _], ev: G <:< Element): Traversal[D, G, C] =
//      traversal.onRaw(_.has(fieldSelect(null.asInstanceOf[D]).name, P.neq(mapping.reverse(value))))
    def hasNot[A](fieldSelect: D => FieldSelector[D, A]): Traversal[D, G, C] =
      traversal.onRaw(_.hasNot(fieldSelect(null.asInstanceOf[D]).name))
    def isEmptyId(fieldSelect: D => FieldSelector[D, EntityId]): Traversal[D, G, C] =
      traversal.onRaw(_.has(fieldSelect(null.asInstanceOf[D]).name, P.eq("")))

    def nonEmptyId(fieldSelect: D => FieldSelector[D, EntityId]): Traversal[D, G, C] =
      traversal.onRaw(_.has(fieldSelect(null.asInstanceOf[D]).name, P.neq("")))

    def unsafeHas[A](key: String, predicate: P[A]): Traversal[D, G, C] =
      traversal.onRaw(_.has(key, traversal.graph.db.mapPredicate(predicate)))
    def unsafeHas[A](key: String, value: A): Traversal[D, G, C] = unsafeHas(key, P.eq[A](value))
    def unsafeHas[A](key: String): Traversal[D, G, C]           = traversal.onRaw(_.has(key))
    def unsafeHasNot[A](key: String): Traversal[D, G, C]        = traversal.onRaw(_.hasNot(key))

    def hasId(ids: EntityId*): Traversal[D, G, C] =
      ids.map(_.value) match {
        case Seq(head, tail @ _*) => traversal.onRaw(_.hasId(head, tail: _*))
        case _                    => traversal.empty.asInstanceOf[Traversal[D, G, C]]
      }

    def hasNotId(ids: EntityId*): Traversal[D, G, C] =
      if (ids.isEmpty) traversal
      else if (ids.sizeIs == 1) traversal.onRaw(_.hasId(P.neq(ids.head.value)))
      else traversal.onRaw(_.hasId(P.without(ids.map(_.value): _*)))

    def label: Traversal[String, String, IdentityConverter[String]] =
      traversal.onRawMap[String, String, IdentityConverter[String]](_.label())(Converter.identity[String])

    def _id: Traversal[EntityId, AnyRef, Converter[EntityId, AnyRef]] =
      traversal.onRawMap[EntityId, AnyRef, Converter[EntityId, AnyRef]](_.id())(EntityId.apply _)

    def update[V](fieldSelect: D => FieldSelector[D, V], value: V)(implicit mapping: Mapping[V, _, _]): Traversal[D, G, C] =
      mapping.setProperty(traversal, fieldSelect(null.asInstanceOf[D]).name, value)

    def value[SDD, DD, GG](
        fieldSelect: D => FieldSelector[D, DD]
    )(implicit mapping: Mapping[DD, SDD, GG]): Traversal[SDD, GG, Converter[SDD, GG]] =
      traversal.property(fieldSelect(null.asInstanceOf[D]).name, mapping)

    def property[DD, GG](name: String, converter: Converter[DD, GG]): Traversal[DD, GG, Converter[DD, GG]] =
      traversal.onRawMap[DD, GG, Converter[DD, GG]](_.values[GG](name))(converter)
    //    def property[DD, GG](name: String, mapping: Mapping[DD, GG]): Traversal[DD, GG, Converter[DD, GG]] =
    //      new Traversal[DD, GG, Converter[DD, GG]](raw.values[GG](name), mapping)
    def _createdBy: Traversal[String, String, Converter[String, String]] = property("_createdBy", UMapping.string)
    def _createdAt: Traversal[Date, Date, Converter[Date, Date]]         = property("_createdAt", UMapping.date)
    def _updatedBy: Traversal[String, String, Converter[String, String]] = property("_updatedBy", UMapping.string)
    def _updatedAt: Traversal[Date, Date, Converter[Date, Date]]         = property("_updatedAt", UMapping.date)
    def markAsUpdatedBy(user: String): Traversal[D, G, C] = {
      val setUpdatedBy: Traversal[D, G, C] => Traversal[D, G, C] = UMapping.string.setProperty(_, "_updatedBy", user)
      def setUpdatedAt: Traversal[D, G, C] => Traversal[D, G, C] = UMapping.date.setProperty(_, "_updatedBy", new Date)
      (setUpdatedBy andThen setUpdatedAt)(traversal)
    }

    def getEntity(entity: D with Entity): Traversal[D, G, C] = getByIds(entity._id)
    def getElement(element: Element): Traversal[D, G, C]     = traversal.onRaw(_.hasId(element.id()))
    def getByIds(ids: EntityId*): Traversal[D, G, C]         = hasId(ids: _*)
  }
}
