package org.thp.scalligraph.query

import scala.language.experimental.macros
import scala.reflect.runtime.{universe ⇒ ru}

import gremlin.scala.{Graph, GremlinScala}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.macros.QueryMacro
import org.thp.scalligraph.models.ScalliSteps
import org.thp.scalligraph.{BadRequestError, RichType}

abstract class QueryExecutor[O] {
  val publicProperties: Seq[PublicProperty[_, _]] = Nil

  def execute[T](query: InitQuery[T])(implicit authGraph: AuthGraph): O = toOutput(query(authGraph))

  def toOutput(output: Any): O                    = sys.error(s"${output.getClass} is invalid for output")

  val sort: QueryBuilder[InputSort, TraversalQuery[_]]     = Query.generic("sort")((param: InputSort) ⇒ param.toQuery(publicProperties))
  val filter: QueryBuilder[InputFilter, TraversalQuery[_]] = Query.generic("filter")((param: InputFilter) ⇒ param.toQuery(publicProperties))
  val toList: QueryBuilder[Unit, GenericQuery]             = Query.generic("toList")((_: Unit) ⇒ ToListQuery)
}

object QueryExecutor {
  def fieldsParser[Q <: QueryExecutor[_]](queryExecutor: Q): FieldsParser[InitQuery[Any]] = macro QueryMacro.queryBuilderParser[Q]
}

abstract class InitQuery[+T](val name: String, val toType: ru.Type) extends (AuthGraph ⇒ T) {
  def this(name: String)(implicit toTag: ru.TypeTag[T]) = this(name, toTag.tpe)
  override def apply(authGraph: AuthGraph): T = apply(authGraph.graph, authGraph.auth)
  def apply(graph: Graph, authContext: Option[AuthContext]): T
}

object InitQuery {
  def apply[T: ru.TypeTag](name: String)(builder: (Graph, Option[AuthContext]) ⇒ T): InitQuery[T] =
    new InitQuery[T](name, ru.typeOf[T]) {
      override def apply(graph: Graph, authContext: Option[AuthContext]): T = builder(graph, authContext)
    }

  def apply[T](name: String, toType: ru.Type)(builder: (Graph, Option[AuthContext]) ⇒ T): InitQuery[T] =
    new InitQuery[T](name, toType) {
      override def apply(graph: Graph, authContext: Option[AuthContext]): T = builder(graph, authContext)
    }

  def build[P, T: ru.TypeTag](name: String)(builder: (P, Graph, Option[AuthContext]) ⇒ T): InitQueryBuilder[P, T] =
    new InitQueryBuilder[P, T](name) {
      override def apply(p: P): InitQuery[T] = new InitQuery[T](name) {
        override def apply(graph: Graph, authContext: Option[AuthContext]): T = builder(p, graph, authContext)
      }
    }
}

abstract class InitQueryBuilder[P, T](name: String) extends (P ⇒ InitQuery[T])

trait QueryComposable[+T] {
  def compose(q: InitQuery[_]): InitQuery[T]
}

abstract class QueryBuilder[P, Q <: QueryComposable[_]](name: String) extends (P ⇒ Q)

abstract class Query[-F, +T](name: String, val fromType: ru.Type, val toType: ru.Type) extends QueryComposable[T] {
  thisQuery ⇒

  def this(name: String)(implicit fromTag: ru.TypeTag[F], toTag: ru.TypeTag[T]) = this(name, fromTag.tpe, toTag.tpe)

  override def compose(q: InitQuery[_]): InitQuery[T] =
    if (q.toType <:< fromType) {
      val qf = q.asInstanceOf[InitQuery[F]]
      InitQuery[T](s"${q.name}_$name", toType)((graph, authContext) ⇒ apply(qf(graph, authContext))(authContext))
    } else throw BadRequestError(s"Query $name can't be applied to ${q.toType}")

  def apply(f: F)(implicit authContext: Option[AuthContext]): T
}

object Query {
  def apply[F: ru.TypeTag, T: ru.TypeTag](name: String)(fn: F ⇒ T): Query[F, T] = new Query[F, T](name) {
    override def apply(f: F)(implicit authContext: Option[AuthContext]): T = fn(f)
  }

  def build[P, F: ru.TypeTag, T: ru.TypeTag](name: String)(builder: (P, F, Option[AuthContext]) ⇒ T): QueryBuilder[P, Query[F, T]] =
    new QueryBuilder[P, Query[F, T]](name) {
      override def apply(p: P): Query[F, T] = new Query[F, T](name) {
        override def apply(f: F)(implicit authContext: Option[AuthContext]): T = builder(p, f, authContext)
      }
    }

  def generic[P, Q <: QueryComposable[_]](name: String)(builder: P ⇒ Q): QueryBuilder[P, Q] =
    new QueryBuilder[P, Q](name) {
      override def apply(p: P): Q = builder(p)
    }
}

abstract class GenericQuery(val name: String) extends QueryComposable[Any] { thisQuery ⇒
  def checkFrom(t: ru.Type): Boolean
  def toType(t: ru.Type): ru.Type
  override def compose(q: InitQuery[_]): InitQuery[Any] =
    if (checkFrom(q.toType))
      InitQuery[Any](s"${q.name}_$name", toType(q.toType))((graph, authContext) ⇒ thisQuery(q(graph, authContext))(authContext))
    else throw BadRequestError(s"Query $name can't be applied on ${q.toType}")
  def apply(f: Any)(implicit authContext: Option[AuthContext]): Any
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

  override def apply(a: Any)(implicit authContext: Option[AuthContext]): Any = a match {
    case s: ScalliSteps[_, _, _] ⇒ s.toList
    case s: GremlinScala[_]      ⇒ s.toList
  }
}

object FNamedObj {
  def unapply(field: Field): Option[(String, FObject)] = field match {
    case f: FObject ⇒
      f.get("_name") match {
        case FString(name) ⇒ Some(name → (f - "_name"))
        case _             ⇒ None
      }
    case _ ⇒ None
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
