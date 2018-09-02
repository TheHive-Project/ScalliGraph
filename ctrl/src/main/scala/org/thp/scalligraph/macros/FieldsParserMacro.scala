package org.thp.scalligraph.macros

import org.thp.scalligraph.controllers.{Attachment, FieldsParser, UpdateFieldsParser, WithParser, WithUpdateParser}
import org.thp.scalligraph.{MacroLogger, MacroUtil}

import scala.reflect.macros.blackbox

/**
  * This class build FieldsParser from CreationRecord or DTO
  */
class FieldsParserMacro(val c: blackbox.Context) extends MacroUtil with MacroLogger {

  import c.universe._

  def getFieldsParser[E: WeakTypeTag]: Tree = {
    val eType = weakTypeOf[E]
    initLogger(eType.typeSymbol)
    _getFieldsParser(eType.typeSymbol, eType)
      .getOrElse(c.abort(c.enclosingPosition, s"Build FieldsParser of $eType fails"))
  }

  private def _getFieldsParser(symbol: Symbol, eType: Type): Option[Tree] = {
    debug(s"getFieldsParser($symbol, $eType")
    if (eType <:< typeOf[Attachment]) {
      Some(q"org.thp.scalligraph.controllers.FieldsParser.attachment")
    } else
      getParserFromAnnotation(symbol, eType)
        .orElse(getParserFromImplicit(eType))
        .orElse(buildParser(eType))
  }

  private def getParserFromAnnotation(symbol: Symbol, eType: Type): Option[Tree] = {
    val withParserType = appliedType(weakTypeOf[WithParser[_]], eType)
    (symbol.annotations ::: eType.typeSymbol.annotations)
      .find(_.tree.tpe <:< withParserType)
      .map(annotation ⇒ annotation.tree.children.tail.head)
  }

  private def getParserFromImplicit(eType: Type): Option[Tree] = {
    val fieldsParserType =
      appliedType(typeOf[FieldsParser[_]].typeConstructor, eType)
    val fieldsParser = c.inferImplicitValue(fieldsParserType, silent = true)
    trace(s"getParserFromImplicit($eType): search implicit of $fieldsParserType => $fieldsParser")
    if (fieldsParser.tpe =:= NoType) None
    else Some(fieldsParser)
  }

  private def buildParser(eType: Type): Option[Tree] =
    eType match {
      case CaseClassType(paramSymbols @ _*) ⇒
        trace(s"build FieldsParser case class $eType")
        val companion = eType.typeSymbol.companion
        val initialBuilder = paramSymbols.length match {
          case 0 ⇒ q"$companion.apply()"
          case 1 ⇒ q"($companion.apply _)"
          case _ ⇒ q"($companion.apply _).curried"
        }
//          if (paramSymbols.length > 1) q"($companion.apply _).curried"
//          else q"($companion.apply _)"
        val entityBuilder = paramSymbols
          .foldLeft[Option[Tree]](Some(q"org.scalactic.Good($initialBuilder).orBad[org.scalactic.Every[org.thp.scalligraph.AttributeError]]")) {
            case (maybeBuilder, s) ⇒
              val symbolName = s.name.toString
              for {
                builder ← maybeBuilder
                parser  ← _getFieldsParser(s, s.typeSignature)
              } yield {
                val builderName = TermName(c.freshName())
                q"""
                  import org.scalactic.{ Bad, Every }
                  import org.thp.scalligraph.AttributeError

                  val $builderName = $builder
                  $parser.apply(path / $symbolName, field.get($symbolName)).fold(
                    param => $builderName.map(_.apply(param)),
                    error => $builderName match {
                      case Bad(errors: Every[_]) => Bad(errors.asInstanceOf[Every[AttributeError]] ++ error)
                      case _ => Bad(error)
                    })
                """
              }
          }

        entityBuilder.map { builder ⇒
          val className: String = eType.toString.split("\\.").last
          q"""
            import org.thp.scalligraph.controllers.FieldsParser

            FieldsParser[$eType]($className) { case (path, field) => $builder }
          """
        }
      case SeqType(subType) ⇒
        trace(s"build FieldsParser sequence of $subType")
        _getFieldsParser(subType.typeSymbol, subType).map { parser ⇒
          q"$parser.sequence"
        }
      case OptionType(subType) ⇒
        trace(s"build FieldsParser option of $subType")
        _getFieldsParser(subType.typeSymbol, subType).map { parser ⇒
          q"$parser.optional"
        }
      case EnumType(values @ _*) ⇒
        trace(s"build FieldsParser enumeration of ${values.map(_._1).mkString("[", ",", "]")}")
        val caseValues = values
          .map {
            case (name, value) ⇒ cq"$name ⇒ $value"
          } :+
          cq"""other ⇒ throw org.thp.scalligraph.InternalError(
              "Wrong value " + other +
              " for numeration " + ${eType.toString} +
              ". Possible values are " + ${values.map(_._1).mkString(",")})"""
        Some(q"""org.thp.scalligraph.controllers.FieldsParser.string.map("enum") { case ..$caseValues }""")
      case _ ⇒
        c.abort(c.enclosingPosition, s"Can't build parser for $eType (${eType.typeSymbol.fullName})")
    }

  /** ***********************************************/
  private def getUpdateParserFromAnnotation(symbol: Symbol, eType: Type): Option[Tree] = {
    val withUpdateParserType =
      appliedType(weakTypeOf[WithUpdateParser[_]], eType)
    (symbol.annotations ::: eType.typeSymbol.annotations)
      .find(_.tree.tpe <:< withUpdateParserType)
      .map(annotation ⇒ annotation.tree.children.tail.head)
  }

  private def getUpdateParserFromImplicit(eType: Type): Option[Tree] = {
    val fieldsParserType =
      appliedType(typeOf[UpdateFieldsParser[_]].typeConstructor, eType)
    val fieldsParser = c.inferImplicitValue(fieldsParserType, silent = true)
    if (fieldsParser.tpe =:= NoType) None
    else Some(fieldsParser)
  }

  private def buildUpdateParser(eType: Type): Tree = {
    val className: String = eType.toString.split("\\.").last
    val updateFieldsParser = _getFieldsParser(eType.typeSymbol, eType)
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
        val subParser = _getUpdateFieldsParser(subType.typeSymbol, subType)
        _getFieldsParser(subType.typeSymbol, subType)
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
        val parser = _getUpdateFieldsParser(subType.typeSymbol, subType)
        q"$parser ++ $updateFieldsParser"
      case CaseClassType(symbols @ _*) ⇒
        symbols.foldLeft(updateFieldsParser) {
          case (parser, s) ⇒
            val symbolName = s.name.toString
            val subParser  = _getUpdateFieldsParser(s, s.typeSignature)
            q"$parser ++ $subParser.on($symbolName)"
        }
      case _ ⇒ updateFieldsParser
    }
  }

  def getUpdateFieldsParser[E: WeakTypeTag]: Tree = {
    val eType = weakTypeOf[E]
    _getUpdateFieldsParser(eType.typeSymbol, eType)
  }

  private def _getUpdateFieldsParser(symbol: Symbol, eType: Type): Tree =
    getUpdateParserFromAnnotation(symbol, eType)
      .orElse(getUpdateParserFromImplicit(eType))
      .getOrElse(buildUpdateParser(eType))
}
