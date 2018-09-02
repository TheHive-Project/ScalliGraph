package org.thp.scalligraph

import scala.reflect.macros.blackbox

trait MacroUtil extends MacroLogger {
  val c: blackbox.Context

  import c.universe._

  object CaseClassType {
    def unapplySeq(tpe: Type): Option[Seq[Symbol]] =
      unapplySeq(tpe.typeSymbol)

    def unapplySeq(s: Symbol): Option[Seq[Symbol]] =
      if (s.isClass) {
        val c = s.asClass
        if (c.isCaseClass)
          Some(c.primaryConstructor.typeSignature.paramLists.head)
        else None
      } else None
  }

  object SeqType {
    def unapply(s: Symbol): Option[Type] =
      s match {
        case _: TypeSymbol ⇒ unapply(s.asType.toType)
        case _: TermSymbol ⇒ unapply(s.typeSignature)
      }

    def unapply(tpe: Type): Option[Type] =
      if (tpe <:< weakTypeOf[Seq[_]]) {
        val TypeRef(_, _, List(subElementType)) = tpe
        Some(subElementType)
      } else None
  }

  object OptionType {
    def unapply(s: Symbol): Option[Type] =
      s match {
        case _: TypeSymbol ⇒ unapply(s.asType.toType)
        case _: TermSymbol ⇒ unapply(s.typeSignature)
      }

    def unapply(tpe: Type): Option[Type] =
      if (tpe <:< weakTypeOf[Option[_]]) {
        val TypeRef(_, _, List(subElementType)) = tpe
        Some(subElementType)
      } else None
  }

  def getTypeArgs(t: Type, fromType: Type): List[Type] =
    if (t.erasure.typeSymbol == fromType.erasure.typeSymbol)
      t.typeArgs
    else
      internal
        .thisType(t.dealias.typeSymbol)
        .baseType(fromType.typeSymbol.asClass)
        .typeArgs

  def symbolToType(symbol: Symbol): Type =
    if (symbol.isType) symbol.asType.toType
    else symbol.typeSignature

  def traverseEntity[E: WeakTypeTag, A](init: A)(f: (Tree, Symbol, A) ⇒ (List[(Tree, Symbol)], A)): A = {

    def unfold(pathSymbolQueue: List[(Tree, Symbol)], currentAcc: A)(f: (Tree, Symbol, A) ⇒ (List[(Tree, Symbol)], A)): A =
      if (pathSymbolQueue.isEmpty) currentAcc
      else {
        val (listOfPathSymbol, nextAcc) = pathSymbolQueue
          .foldLeft[(List[(Tree, Symbol)], A)]((Nil, currentAcc)) {
            case ((newPathSymbolQueue, acc), (path, symbol)) ⇒
              val (nextSymbols, a) = f(path, symbol, acc)
              (newPathSymbolQueue ::: nextSymbols) → a
          }
        unfold(listOfPathSymbol, nextAcc)(f)
      }

    unfold(List(q"org.thp.scalligraph.FPath.empty" → weakTypeOf[E].typeSymbol), init)(f)
  }

  object EnumType {
    def unapplySeq(tpe: Type): Option[Seq[(Tree, Tree)]] =
      extractEnum(tpe, tpe.typeSymbol)

    def unapplySeq(sym: Symbol): Option[Seq[(Tree, Tree)]] =
      if (sym.isType) extractEnum(sym.asType.toType, sym)
      else extractEnum(sym.typeSignature, sym)

    def enumerationType(tpe: Type, sym: Symbol): Option[Seq[Symbol]] =
      if (tpe <:< typeOf[Enumeration#Value]) {
        val members = tpe
          .asInstanceOf[TypeRef]
          .pre
          .members
          .filter(s ⇒ s.isTerm && !(s.isMethod || s.isModule || s.isClass) && (s.typeSignature.resultType <:< typeOf[Enumeration#Value]))
          .toList
        if (members.isEmpty) {
          trace(s"$tpe is an enumeration but value list is empty")
          None
        } else {
          trace(s"$tpe is an enumeration with value: ${members.map(_.name.toString)}")
          Some(members)
        }
      } else {
        trace(s"$tpe is not an Enumeration")
        None
      }

    def sealedType(s: Symbol): Option[Seq[Symbol]] =
      if (s.isModule || s.isModuleClass) Some(List(s))
      else if (s.isClass) {
        val cs = s.asClass

        if ((cs.isTrait || cs.isAbstract) && cs.isSealed) {
          trace(s"searching direct sub classes for $cs")
          cs.knownDirectSubclasses.foldLeft[Option[List[Symbol]]](Some(Nil)) {
            case (None, _) ⇒
              trace(s" ==> error found")
              None
            case (Some(set), knownSubclass) ⇒
              trace(s" ==> $knownSubclass found")
              sealedType(knownSubclass).map(set ++ _)
          }
        } else {
          trace(s"$s is a class but it is not sealed")
          None
        }
      } else {
        trace(s"$s is not a class")
        None
      }

    def extractEnum(tpe: Type, sym: Symbol): Option[Seq[(Tree, Tree)]] =
      (enumerationType(tpe, sym) orElse sealedType(sym)).map(_.map { value ⇒
        val valueName = q"${value.name.decodedName.toString.trim}" // q"$value.toString"
        if (value.isModuleClass) {
          if (value.owner.isModuleClass) {
            val v = value.owner.typeSignature.member(value.name.toTermName)
            if (value.asClass.toType.member(TermName("name")) == NoSymbol)
              valueName → q"$v"
            else
              q"$v.name" → q"$v"
//            valueName -> q"$v"
          } else {
            valueName → q"${value.name.toTermName}"
          }
        } else {
          val moduleClass = tpe.asInstanceOf[TypeRef].pre.typeSymbol
          val ownerSymbol = moduleClass.owner.typeSignature.member(moduleClass.name.toTermName)
          valueName → q"$ownerSymbol.${value.asTerm.getter}"
        }
      })
  }

}
