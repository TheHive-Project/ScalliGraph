package org.thp.scalligraph.macros

import scala.reflect.macros.blackbox

import org.thp.scalligraph.models._

class MappingMacro(val c: blackbox.Context) extends MappingMacroHelper with MacroLogger {
  import c.universe._

  def getOrBuildMapping[E: WeakTypeTag]: Tree = {
    val eType = weakTypeOf[E]
    if (eType <:< typeOf[Enumeration#Value]) initLogger(eType.asInstanceOf[TypeRef].pre.typeSymbol)
    else initLogger(eType.typeSymbol)
    ret(
      s"Mapping of $eType",
      getMapping(eType.typeSymbol, eType)
    )
  }
}

trait MappingMacroHelper extends MacroUtil with MacroLogger {
  val c: blackbox.Context

  import c.universe._

  case class MappingSymbol(name: String, valName: TermName, definition: Tree)

  def getEntityMappings[E: WeakTypeTag]: Seq[MappingSymbol] = {

    val eType = weakTypeOf[E]
    eType match {
      case CaseClassType(symbols @ _*) =>
        symbols.map(s => MappingSymbol(s.name.decodedName.toString.trim, TermName(c.freshName(s.name + "Mapping")), getMapping(s, s.typeSignature)))
    }
  }

  def getMapping(symbol: Symbol, eType: Type): Tree = {
    val mapping = getMappingFromAnnotation(symbol, eType)
      .orElse(getMappingFromImplicit(eType))
      .orElse(buildMapping(symbol, eType))
      .getOrElse(c.abort(c.enclosingPosition, s"Fail to get mapping of $symbol ($eType)"))
    symbol.annotations.find(_.tree.tpe <:< typeOf[Readonly]).fold(mapping) { _ =>
      q"$mapping.setReadonly(true)"
    }
  }

  private def getMappingFromAnnotation(symbol: Symbol, eType: Type): Option[Tree] = {
    val mappingAnnotationType =
      appliedType(weakTypeOf[WithMapping[_, _]].typeConstructor, eType, typeOf[Any])
    (symbol.annotations ::: eType.typeSymbol.annotations)
      .find(_.tree.tpe <:< mappingAnnotationType)
      .map(annotation => annotation.tree.children.tail.head)
  }

  private def getMappingFromImplicit(eType: Type): Option[Tree] = {
    val mappingType = appliedType(typeOf[UniMapping[_]].typeConstructor, eType)
    val mapping     = c.inferImplicitValue(mappingType, silent = true, withMacrosDisabled = true)
    if (mapping.tpe =:= NoType) None
    else Some(mapping)
  }

  private def buildMapping(symbol: Symbol, eType: Type): Option[Tree] =
    eType match {
      case EnumerationType(members @ _*) =>
        val valueCases = members.map {
          case (name, value) => cq"$name ⇒ $value"
        } :+
          cq"""other ⇒ throw org.thp.scalligraph.InternalError(
              "Wrong value " + other +
              " for numeration " + ${symbol.toString} +
              ". Possible values are " + ${members.map(_._1).mkString(",")})"""
        Some(q"""org.thp.scalligraph.models.SingleMapping[$eType, String]("", e ⇒ Some(e.toString), g ⇒ g match { case ..$valueCases })""")
      case _ => None
    }
}
