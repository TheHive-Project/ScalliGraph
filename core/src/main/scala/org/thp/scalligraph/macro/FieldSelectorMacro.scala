package org.thp.scalligraph.`macro`

import scala.reflect.macros.blackbox

class FieldSelectorMacro(val c: blackbox.Context) extends MacroLogger with MacroUtil {

  import c.universe._

  def getSelectorName(tree: Tree): Option[String] =
    tree match {
      case Select(_, fieldName) => Some(fieldName.toString)
      case _                    => None
    }

  def getFieldType[S](field: String)(implicit sTypeTag: WeakTypeTag[S]): Option[Type] = {
    val fields = sTypeTag.tpe match {
      case CaseClassType(f @ _*)                           => f
      case RefinedType(CaseClassType(f @ _*) :: t :: _, _) => f ++ t.members.filter(_.isTerm)
      case _                                               => Nil
    }
    fields.find(f => f.name.toString == field).map(_.typeSignature)
  }

  def selectorProviderImpl[S: WeakTypeTag, T: WeakTypeTag](select: Tree): Tree =
    getSelectorName(select).fold(fatal(s"Invalid tree: $select")) { name =>
      getFieldType[S](name).fold(fatal(s"Class ${weakTypeOf[S]} doesn't contain field $name")) { tpe =>
        if (weakTypeOf[T] <:< tpe)
//          println(s"FOUND field $name with type ${tpe} from ${weakTypeOf[T]}")
          q"org.thp.scalligraph.utils.FieldSelector($name)"
        else fatal(s"The field $name has not the expected type (${weakTypeOf[T]}, expected: $tpe)")
      }
    }
}
