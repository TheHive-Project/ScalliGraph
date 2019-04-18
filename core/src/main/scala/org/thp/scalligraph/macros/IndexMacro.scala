package org.thp.scalligraph.macros

import scala.reflect.macros.blackbox

import org.thp.scalligraph.models.DefineIndex

trait IndexMacro {
  val c: blackbox.Context

  import c.universe._

  def getIndexes[E: WeakTypeTag]: Tree = {
    val eType = weakTypeOf[E]
    val indexes = eType.typeSymbol.annotations.collect {
      case annotation if annotation.tree.tpe <:< typeOf[DefineIndex] ⇒
        val args      = annotation.tree.children.tail
        val indexType = args.head
        val fields    = args.tail
        q"$indexType → $fields"
    }
    q"Seq(..$indexes)"
  }
}
