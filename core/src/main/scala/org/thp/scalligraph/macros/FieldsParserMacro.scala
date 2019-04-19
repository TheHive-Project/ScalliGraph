package org.thp.scalligraph.macros

import scala.reflect.macros.blackbox

import org.thp.scalligraph.controllers._
import org.thp.scalligraph.{MacroLogger, MacroUtil}

class FieldsParserMacro(val c: blackbox.Context) extends MacroLogger with UpdateFieldsParserUtil {

  import c.universe._

  def getOrBuildFieldsParser[E: WeakTypeTag]: Tree = {
    val eType = weakTypeOf[E]
    if (eType <:< typeOf[Enumeration#Value]) initLogger(eType.asInstanceOf[TypeRef].pre.typeSymbol)
    else initLogger(eType.typeSymbol)
    if (eType <:< typeOf[FFile]) {
      q"org.thp.scalligraph.controllers.FieldsParser.file"
    } else
      ret(s"FieldParser of $eType",
        getParserFromAnnotation(eType.typeSymbol, eType)
        .orElse(getParserFromImplicit(eType))
        .orElse(buildParser(eType))
        .getOrElse(c.abort(c.enclosingPosition, s"Build FieldsParser of $eType fails")))
  }

  def getOrBuildUpdateFieldsParser[E: WeakTypeTag]: Tree = {
    val eType = weakTypeOf[E]
    initLogger(eType.typeSymbol)
    ret(s"UpdateFieldParser of $eType", getOrBuildUpdateParser(eType.typeSymbol, eType))
  }
}

trait FieldsParserUtil extends MacroLogger with MacroUtil {
  val c: blackbox.Context

  import c.universe._

  protected def getOrBuildParser(symbol: Symbol, eType: Type): Option[Tree] = {
    debug(s"getOrBuildParser($symbol, $eType)")
    if (eType <:< typeOf[FFile]) {
      Some(q"org.thp.scalligraph.controllers.FieldsParser.file")
    } else
      getParserFromAnnotation(symbol, eType)
        .orElse(getParserFromImplicit(eType))
        .orElse(buildParser(eType))
  }

  protected def getParserFromAnnotation(symbol: Symbol, eType: Type): Option[Tree] = {
    val withParserType = appliedType(typeOf[WithParser[_]], eType)
    (symbol.annotations ::: eType.typeSymbol.annotations)
      .map { a ⇒
        debug(s"Found annotation [$a] for $symbol / $eType")
        a
      }
      .find(_.tree.tpe <:< withParserType)
      .map(annotation ⇒ annotation.tree.children.tail.head)
  }

  protected def getParserFromImplicit(eType: Type): Option[Tree] = {
    val fieldsParserType =
      appliedType(typeOf[FieldsParser[_]].typeConstructor, eType)
    val fieldsParser = c.inferImplicitValue(fieldsParserType, silent = true)
    trace(s"getParserFromImplicit($eType): search implicit of $fieldsParserType ⇒ $fieldsParser")
    if (fieldsParser.tpe =:= NoType) None
    else Some(fieldsParser)
  }

  protected def buildParser(eType: Type): Option[Tree] =
    eType match {
      case CaseClassType(paramSymbols @ _*) ⇒
        trace(s"build FieldsParser case class $eType")
        val companion = eType.typeSymbol.companion
        val initialBuilder = paramSymbols.length match {
          case 0 ⇒ q"$companion.apply()"
          case 1 ⇒ q"($companion.apply _)"
          case _ ⇒ q"($companion.apply _).curried"
        }
        val entityBuilder = paramSymbols
          .foldLeft[Option[Tree]](Some(q"org.scalactic.Good($initialBuilder).orBad[org.scalactic.Every[org.thp.scalligraph.AttributeError]]")) {
            case (maybeBuilder, s) ⇒
              val symbolName = s.name.toString
              for {
                builder ← maybeBuilder
                parser  ← getOrBuildParser(s, s.typeSignature)
              } yield {
                val builderName = TermName(c.freshName())
                q"""
                  import org.scalactic.{ Bad, Every }
                  import org.thp.scalligraph.AttributeError

                  val $builderName = $builder
                  $parser.apply(path / $symbolName, field.get($symbolName)).fold(
                    param ⇒ $builderName.map(_.apply(param)),
                    error ⇒ $builderName match {
                      case Bad(errors: Every[_]) ⇒ Bad(errors.asInstanceOf[Every[AttributeError]] ++ error)
                      case _ ⇒ Bad(error)
                    })
                """
              }
          }

        entityBuilder.map { builder ⇒
          val className: String = eType.toString.split("\\.").last
          q"""
            import org.thp.scalligraph.controllers.FieldsParser

            FieldsParser[$eType]($className) { case (path, field) ⇒ $builder }
          """
        }
      case SeqType(subType) ⇒
        trace(s"build FieldsParser sequence of $subType")
        getOrBuildParser(subType.typeSymbol, subType).map { parser ⇒
          q"$parser.sequence"
        }
      case SetType(subType) ⇒
        trace(s"build FieldsParser set of $subType")
        getOrBuildParser(subType.typeSymbol, subType).map { parser ⇒
          q"$parser.set"
        }
      case OptionType(subType) ⇒
        trace(s"build FieldsParser option of $subType")
        getOrBuildParser(subType.typeSymbol, subType).map { parser ⇒
          q"$parser.optional"
        }
      case EnumerationType(values @ _*) ⇒
        trace(s"build FieldsParser enumeration of ${values.map(_._1).mkString("[", ",", "]")}")
        val caseValues = values
          .map {
            case (name, value) ⇒ cq"(_, org.thp.scalligraph.controllers.FString($name)) ⇒ org.scalactic.Good($value)"
          }
        Some(q"org.thp.scalligraph.controllers.FieldsParser(${eType.toString}, Set(${values.map(_._1): _*})) { case ..$caseValues }")
      case _ ⇒
        None
    }
}

trait UpdateFieldsParserUtil extends FieldsParserUtil {
  val c: blackbox.Context

  import c.universe._

  protected def getOrBuildUpdateParser(symbol: Symbol, eType: Type): Tree = {
    debug(s"getOrBuildUpdateParser($symbol, $eType")
    getUpdateParserFromAnnotation(symbol, eType)
      .orElse(getUpdateParserFromImplicit(eType))
      .getOrElse(buildUpdateParser(symbol, eType))
  }

  protected def getUpdateParserFromAnnotation(symbol: Symbol, eType: Type): Option[Tree] = {
    val withUpdateParserType =
      appliedType(weakTypeOf[WithUpdateParser[_]], eType)
    (symbol.annotations ::: eType.typeSymbol.annotations)
      .find(_.tree.tpe <:< withUpdateParserType)
      .map(annotation ⇒ annotation.tree.children.tail.head)
  }

  protected def getUpdateParserFromImplicit(eType: Type): Option[Tree] = {
    val fieldsParserType =
      appliedType(typeOf[UpdateFieldsParser[_]].typeConstructor, eType)
    val fieldsParser = c.inferImplicitValue(fieldsParserType, silent = true)
    trace(s"getParserFromImplicit($eType): search implicit of $fieldsParserType ⇒ $fieldsParser")
    if (fieldsParser.tpe =:= NoType) None
    else Some(fieldsParser)
  }

  protected def buildUpdateParser(symbol: Symbol, eType: Type): Tree = {
    val className: String = eType.toString.split("\\.").last
    val updateFieldsParser = getOrBuildParser(symbol, eType)
      .map { parser ⇒
        q"""
          import org.thp.scalligraph.controllers.UpdateFieldsParser
          import org.thp.scalligraph.FPath
          UpdateFieldsParser[$eType]($className, Map(FPath.empty -> $parser.toUpdate))
        """
      }
      .getOrElse(q"org.thp.scalligraph.controllers.UpdateFieldsParser.empty[$eType]")

    eType match {
      case SeqType(subType) ⇒
        trace(s"build UpdateFieldsParser sequence of $subType")
        val subParser = getOrBuildUpdateParser(subType.typeSymbol, subType)
        getOrBuildParser(subType.typeSymbol, subType)
          .map { parser ⇒
            q"""
              import org.thp.scalligraph.controllers.UpdateFieldsParser
              import org.thp.scalligraph.FPath
              UpdateFieldsParser($className, Map(FPath.seq -> $parser.toUpdate))
            """
          }
          .fold(q"$updateFieldsParser ++ $subParser.sequence") { seqParser ⇒
            q"$updateFieldsParser ++ $subParser.sequence ++ $seqParser"
          }
      case OptionType(subType) ⇒
        trace(s"build UpdateFieldsParser option of $subType")
        val parser = getOrBuildUpdateParser(subType.typeSymbol, subType)
        q"$parser ++ $updateFieldsParser"
      case CaseClassType(symbols @ _*) ⇒
        trace(s"build UpdateFieldsParser case class $eType")
        symbols.foldLeft(updateFieldsParser) {
          case (parser, s) ⇒
            val symbolName = s.name.toString
            val subParser  = getOrBuildUpdateParser(s, s.typeSignature)
            q"$parser ++ $subParser.on($symbolName)"
        }
      case _ ⇒ updateFieldsParser
    }
  }
}
