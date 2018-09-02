package org.thp.scalligraph.query

import gremlin.scala.GremlinScala
import org.scalactic.{Bad, Good, One}
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.ScalliSteps
import org.thp.scalligraph.{BadRequestError, InvalidFormatAttributeError, RichType}

import scala.reflect.runtime.{universe ⇒ ru}

abstract class QueryExecutor[O] {
  def outputs: PartialFunction[ru.Type, Any ⇒ O]
  val initQueryParser: FieldsParser[InitQuery[_]]    = FieldsParser.empty
  val queryParser: FieldsParser[Query[_, _]]         = FieldsParser.empty
  val filterParser: FieldsParser[Filter[_]]          = FieldsParser.empty
  val genericQueryParser: FieldsParser[GenericQuery] = FieldsParser.empty

  def getOutputFunction[T](t: ru.Type): T ⇒ O = outputs.applyOrElse(t, (_: ru.Type) ⇒ sys.error(s"type $t is invalid for output")).asInstanceOf[T ⇒ O]

  def execute[T](query: InitQuery[T])(implicit authGraph: AuthGraph): O = {
    val output = getOutputFunction(query.toType)
    output(query(authGraph))
  }
}

abstract class Query[FROM, +TO](val name: String, val fromType: ru.Type, val toType: ru.Type) extends (FROM ⇒ TO) {
  thisQuery ⇒

  def this(name: String)(implicit fromTag: ru.TypeTag[FROM], toTag: ru.TypeTag[TO]) = this(name, fromTag.tpe, toTag.tpe)

  def compose(q: InitQuery[_]): InitQuery[TO] =
    if (q.toType <:< fromType) new InitQuery[TO](s"${q.name}_$name", toType) {
      override def apply(g: AuthGraph): TO = thisQuery.apply(q.apply(g).asInstanceOf[FROM])
    } else ???
}

abstract class GenericQuery(val name: String) extends (Any ⇒ Any) { thisQuery ⇒
  def checkFrom(t: ru.Type): Boolean
  def toType(t: ru.Type): ru.Type
  def compose(q: InitQuery[_]): InitQuery[Any] =
    if (checkFrom(q.toType)) new InitQuery[Any](s"${q.name}_$name", toType(q.toType)) {
      override def apply(g: AuthGraph): Any = thisQuery(q(g))
    } else {
      throw BadRequestError(s"Query $name can't be applied on ${q.toType}")
    }
}

abstract class InitQuery[+TO](val name: String, val toType: ru.Type) extends (AuthGraph ⇒ TO) {
  def this(name: String)(implicit toTag: ru.TypeTag[TO]) = this(name, toTag.tpe)
}

object InitQuery {
  def apply[TO: ru.TypeTag](name: String)(f: AuthGraph ⇒ TO): InitQuery[TO] = new InitQuery[TO](name) {
    override def apply(g: AuthGraph): TO = f(g)
  }
}

object ToListQuery extends GenericQuery("toList") {
  override def checkFrom(t: ru.Type): Boolean = t <:< ru.typeOf[ScalliSteps[_, _, _]] || t <:< ru.typeOf[GremlinScala[_]]

  override def toType(t: ru.Type): ru.Type = {
    val subType = if (t <:< ru.typeOf[ScalliSteps[_, _, _]]) {
      RichType.getTypeArgs(t, ru.typeOf[ScalliSteps[_, _, _]]).head
    } else if (t <:< ru.typeOf[GremlinScala[_]]) {
      RichType.getTypeArgs(t, ru.typeOf[GremlinScala[_]]).head
    } else {
      throw BadRequestError(s"toList can't be used with $t. It must be a ScalliSteps or a GremlinScala")
    }
    ru.appliedType(ru.typeOf[List[_]].typeConstructor, subType)
  }

  override def apply(a: Any): Any = a match {
    case s: ScalliSteps[_, _, _] ⇒ s.toList
    case s: GremlinScala[_]      ⇒ s.toList
  }
}

object FObjOne {
  def unapply(field: Field): Option[(String, Field)] = field match {
    case FObject(f) if f.size == 1 ⇒ Some(f.head)
    case _                         ⇒ None
  }
}

object FNative {
  def unapply(field: Field): Option[Any] = field match {
    case FString(s)  ⇒ Some(s)
    case FNumber(n)  ⇒ Some(n)
    case FBoolean(b) ⇒ Some(b)
    case FAny(a)     ⇒ Some(a.mkString)
    case _           ⇒ None
  }
}

object Query {

  def apply[F: ru.TypeTag, T: ru.TypeTag](name: String)(fn: F ⇒ T): Query[F, T] = new Query[F, T](name) {
    override def apply(f: F): T = fn(f)
  }

  val defaultFilterParser: FieldsParser[Filter[_]] = FieldsParser[Filter[_]]("Filter") {
    case (_, FObjOne("_filter", field)) ⇒ Filter.fieldsParser(field)
    case (_, FObjOne("_sort", field))   ⇒ ???
  }

  val defaultGenericQueryParser: FieldsParser[GenericQuery] = FieldsParser[GenericQuery]("GenericQuery") {
    case (_, FObjOne("_toList", _)) ⇒ Good(ToListQuery)

  }
  val defaultQueryParser: FieldsParser[Query[_, _]] = FieldsParser.empty[Query[_, _]]

  def fieldsParser(executor: QueryExecutor[_]): FieldsParser[InitQuery[_]] =
    fieldsParser(executor.initQueryParser, executor.queryParser, executor.filterParser, executor.genericQueryParser)

  def fieldsParser(
      initQueryParser: FieldsParser[InitQuery[_]],
      queryParser: FieldsParser[Query[_, _]] = FieldsParser.empty,
      filterParser: FieldsParser[Filter[_]] = FieldsParser.empty,
      genericQueryParser: FieldsParser[GenericQuery] = FieldsParser.empty): FieldsParser[InitQuery[_]] =
    initQueryParser
      .orElse {
        FieldsParser[InitQuery[_]]("Query") {
          case (_, FSeq(head :: tail)) ⇒
            tail.foldLeft(initQueryParser(head)) {
              case (Good(query), field) ⇒
                queryParser(field)
                  .map(_.compose(query))
                  .orElse(filterParser(field).map(_.compose(query)))
                  .orElse(genericQueryParser(field).map(_.compose(query)))
                  .orElse(defaultQueryParser(field).map(_.compose(query)))
                  .orElse(defaultFilterParser(field).map(_.compose(query)))
                  .orElse(defaultGenericQueryParser(field).map(_.compose(query)))
              case (bad, _) ⇒ bad
            }
          case (_, field) ⇒ Bad(One(InvalidFormatAttributeError("query", "Query", field)))
        }
      }
}
