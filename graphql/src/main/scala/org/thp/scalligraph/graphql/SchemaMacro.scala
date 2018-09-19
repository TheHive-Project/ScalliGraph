package org.thp.scalligraph.graphql

import org.thp.scalligraph.query.{InitQuery, Query}
import org.thp.scalligraph.{MacroLogger, MacroUtil}

import scala.reflect.macros.blackbox

class SchemaMacro(val c: blackbox.Context) extends OutputTypeMacro with InputTypeMacro with MacroUtil with MacroLogger {

  import c.universe._

  class Definitions(val executor: Tree, val queries: Seq[(MethodSymbol, Type, Type)]) {
    private var inputs: Seq[(Type, Tree)]                        = Nil
    private var definitions: Seq[(Type, TermName, Option[Tree])] = Nil
    private var filters: Seq[(Type, TermName, Tree)]             = Nil

    override def toString: String =
      definitions.map {
        case (tpe, name, v) ⇒ s"\n- $name: $tpe = ${v.isEmpty}"
      }.mkString

    def list: Seq[Tree] =
      inputs.map { case (tpe, value)        ⇒ q"implicit lazy val ${TermName(c.freshName)}: $tpe = $value" } ++
        filters.map { case (_, name, value) ⇒ q"lazy val $name = $value" } ++
        definitions.collect {
          case (tpe, name, Some(value)) ⇒ q"lazy val $name: $tpe = $value"
        }

    def addInput(tpe: Type, value: ⇒ Tree): Unit =
      if (!inputs.exists(tpe =:= _._1))
        inputs :+= tpe → value

    def add(value: Tree): TermName = add(TermName(c.freshName()), NoType, value)

//    def addStatement(value: Tree): Unit = statements :+= value

    def add(name: TermName, tpe: Type, value: Tree): TermName = {
      definitions :+= ((tpe, name, Some(value)))
      name
    }

    def add(name: TermName, tpe: Type): TermName = {
      definitions :+= ((tpe, name, None))
      name
    }

    def remove(name: TermName): Unit =
      definitions = definitions.filterNot(_._2 == name)

    def getOrAdd(tpe: Type)(value: ⇒ Tree): TermName =
      definitions
        .collectFirst {
          case (t, name, _) if t <:< tpe ⇒ name
        }
        .getOrElse {
          val name = TermName(c.freshName())
          add(name, tpe)
          add(name, tpe, value)
        }

    def getOrOptionAdd(tpe: Type)(value: ⇒ Option[Tree]): Option[TermName] =
      definitions
        .collectFirst {
          case (t, name, _) if t <:< tpe ⇒ Some(name)
        }
        .getOrElse {
          val name = TermName(c.freshName())
          add(name, tpe)
          value match {
            case Some(v) ⇒ Some(add(name, tpe, v))
            case None ⇒
              remove(name)
              None
          }
        }

    def getOrAddFilter(tpe: Type)(value: ⇒ Tree): TermName =
      filters
        .collectFirst {
          case (t, name, _) if t <:< tpe ⇒ name
        }
        .getOrElse {
          val name = TermName(c.freshName())
          filters :+= ((tpe, name, value))
          name
        }

  }

  def buildSchema[E: WeakTypeTag](executor: Tree): Tree = {
    val tpe = weakTypeOf[E]
    initLogger(tpe.typeSymbol)
    info(s"Building graphql schema of $executor")

    val initQueryType = typeOf[InitQuery[_]]
    val queryType     = typeOf[Query[_, _]]
    val initQueries   = tpe.decls.collect { case s if s.isMethod && s.asMethod.returnType <:< initQueryType ⇒ s.asMethod }
    val queries = tpe.decls
      .collect {
        case s if s.isMethod && s.asMethod.returnType <:< queryType ⇒
          val m                              = s.asMethod
          val inputType :: outputType :: Nil = getTypeArgs(m.returnType, queryType)
          (m, inputType, outputType)

      }
    implicit val definitions: Definitions = new Definitions(executor, queries.toSeq)

    def buildInitQuery(initQueryMethod: MethodSymbol): Option[Tree] /* sangria.schema.Field */ = {
      debug(s"Build initial query $initQueryMethod")
      val outputType = getTypeArgs(initQueryMethod.returnType, typeOf[InitQuery[_]]).head
      getOutput(outputType).map { outputObjectType ⇒
        val fieldName = initQueryMethod.name.decodedName.toString.trim

        val arguments      = initQueryMethod.paramLists.headOption.getOrElse(Nil).map(buildArgument)
        val argumentValues = arguments.map(arg ⇒ q"ctx.arg($arg)")
        q"""
        sangria.schema.Field(
          $fieldName,
          $outputObjectType,
          arguments = List(..$arguments),
          resolve = ctx => ${definitions.executor}.$initQueryMethod(..$argumentValues)(ctx.ctx))
      """
      }
    }

    val fields = initQueries.map(buildInitQuery)
    ret(
      s"GraphQL schema of $tpe is\n",
      q"""
        ..${definitions.list}
        sangria.schema.Schema(sangria.schema.ObjectType(
          ${tpe.typeSymbol.asClass.name.toString},
          () => sangria.schema.fields[org.thp.scalligraph.query.AuthGraph, Unit](..$fields)))
      """
    )
  }
}
