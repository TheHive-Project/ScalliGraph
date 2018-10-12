package org.thp.scalligraph.macros

import org.thp.scalligraph.models.Entity
import org.thp.scalligraph.{MacroUtil, WithOutput}
import play.api.libs.json.Writes

import scala.reflect.macros.blackbox

class JsonMacro(val c: blackbox.Context) extends MacroUtil {

  import c.universe._

  def getJsonWrites[E: WeakTypeTag]: Tree = {
    val eType = weakTypeOf[E]
    getJsonWritesFromAnnotation(eType.typeSymbol, eType)
      .orElse(buildJsonWrites(eType))
      .getOrElse(c.abort(c.enclosingPosition, s"no writes found for $eType"))
  }

  private def _getJsonWrites(symbol: Symbol, eType: Type): Tree =
    getJsonWritesFromAnnotation(symbol, eType)
      .orElse(getJsonWritesFromImplicit(eType))
      .orElse(buildJsonWrites(eType))
      .getOrElse(c.abort(c.enclosingPosition, s"no writes found for $eType"))

  private def getJsonWritesFromAnnotation(symbol: Symbol, eType: Type): Option[Tree] = {
    val withOutputType = appliedType(weakTypeOf[WithOutput[_]], eType)
    (symbol.annotations ::: eType.typeSymbol.annotations)
      .find(_.tree.tpe <:< withOutputType)
      .map(annotation ⇒ annotation.tree.children.tail.head)
  }

  private def getJsonWritesFromImplicit(eType: Type): Option[Tree] = {
    val writesType = appliedType(weakTypeOf[Writes[_]], eType)
    val writes     = c.inferImplicitValue(writesType, silent = true)
    if (writes.tpe =:= NoType) None
    else Some(writes)
  }

  private def buildJsonWrites(eType: Type): Option[Tree] =
    eType match {
      case RefinedType(cl :: traits, _) ⇒
        val params = cl.decls.collect {
          case symbol if symbol.isMethod && symbol.asMethod.isGetter ⇒
            val symbolName = symbol.name.toString
            val writes     = _getJsonWrites(symbol, symbol.asMethod.returnType)
            q"$symbolName → $writes.writes(e.${TermName(symbolName)})"
        }
        val entityParams =
          if (traits.exists(_ <:< typeOf[Entity]))
            Seq(
              q""""_id"        → play.api.libs.json.JsString(e._id)""",
              q""""_createdAt" → play.api.libs.json.JsNumber(e._createdAt.getTime())""",
              q""""_createdBy" → play.api.libs.json.JsString(e._createdBy)""",
              q""""_updatedAt" → e._updatedAt.fold[play.api.libs.json.JsValue](play.api.libs.json.JsNull)(d ⇒ play.api.libs.json.JsNumber(d.getTime()))""",
              q""""_updatedBy" → e._updatedBy.fold[play.api.libs.json.JsValue](play.api.libs.json.JsNull)(play.api.libs.json.JsString.apply)""",
              q""""_type"      → play.api.libs.json.JsString(e._model.label)"""
            )
          else Nil

        Some(q"""
            import play.api.libs.json.{ JsObject, Writes }
            Writes[$eType]((e: $eType) ⇒ JsObject(Seq(..${params ++ entityParams})))
          """)
      case CaseClassType(symbols @ _*) ⇒
        val params = symbols.map { symbol ⇒
          val symbolName = symbol.name.toString
          val writes     = _getJsonWrites(symbol, symbol.typeSignature)
          q"$symbolName → $writes.writes(e.${TermName(symbolName)})"
        }
        Some(q"""
            import play.api.libs.json.{ JsObject, Writes }
            Writes[$eType]((e: $eType) ⇒ JsObject(Seq(..$params)))
          """)
      case SeqType(subType) ⇒
        val writes = _getJsonWrites(subType.typeSymbol, subType)
        Some(q"""
          import play.api.libs.json.{ JsArray, Writes }
          Writes[$eType]((e: $eType) ⇒ JsArray(e.map($writes.writes)))
         """)
      //      case simpleClass ⇒
      //        val params = simpleClass.decls.collect {
      //          case symbol if symbol.isMethod && symbol.asMethod.isGetter ⇒
      //            val symbolName = symbol.name.toString
      //            val writes     = _getJsonWrites(symbol, symbol.asMethod.returnType)
      //            q"$symbolName → $writes.writes(e.${TermName(symbolName)})"
      //        }
      //        Some(q"""
      //            import play.api.libs.json.{ JsObject, Writes }
      //            Writes[$eType]((e: $eType) ⇒ JsObject(Seq(..$params)))
      //          """)
      // case Option todo
    }
}
