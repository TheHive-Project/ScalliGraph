package org.thp.scalligraph.macros

import org.thp.scalligraph.steps.Traversal

import scala.reflect.macros.blackbox

class UpdateMacro(val c: blackbox.Context) extends MacroUtil {
  import c.universe._

  def getSelectorName(tree: Tree): Option[Name] =
    tree match {
      case q"(${vd: ValDef}) => ${idt: Ident}.${fieldName: Name}" if vd.name == idt.name => Some(fieldName)
      case _                                                                             => None
    }

  def update(selector: Tree, value: Tree)(ev1: Tree, ev2: Tree): Tree = {
    val traversalOps: Tree = c.prefix.tree
    val traversal          = q"$traversalOps.traversal"
    val traversalType      = c.typecheck(traversal).tpe
    val entityType: Type   = traversalType.baseType(typeOf[Traversal.ANY].typeSymbol).typeArgs.head

    entityType match {
      case RefinedType((baseEntityType @ CaseClassType(members @ _*)) :: _, _) =>
        val entityModule: Symbol = baseEntityType.typeSymbol.companion
        val model: Tree          = q"$entityModule.model"
        val name: Name           = getSelectorName(q"$selector").getOrElse(fatal(s"$selector is an invalid selector"))
        val memberType: Type     = members.find(_.name == name).getOrElse(fatal(s"$entityType doesn't contain member $name")).typeSignature
        val valueType: Type      = c.typecheck(q"$value").tpe
        val mapping: Tree        = q"$model.fields(${name.toString}).asInstanceOf[org.thp.scalligraph.models.Mapping[$valueType, _, _]]"
        if (valueType <:< memberType) ret("Update traversal", q"$mapping.setProperty($traversal, ${name.toString}, $value)")
        else fatal(s"Incompatible type for $name, expected $memberType, found $valueType")
      case _ => fatal(s"$entityType is not a valid entity type")
    }
  }
}
