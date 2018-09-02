package org.thp.scalligraph.graphql

import java.util.Date

import scala.reflect.macros.blackbox

trait InputTypeMacro { _: SchemaMacro ⇒
  val c: blackbox.Context

  import c.universe._

  object ImplicitInputType {
    def unapply(tpe: Type): Option[Tree] = {
      val oututType   = appliedType(typeOf[sangria.schema.InputType[_]].typeConstructor, tpe)
      val outputValue = c.inferImplicitValue(oututType, silent = true)
      if (outputValue.tpe =:= NoType) None
      else Some(outputValue)
    }
  }

  def getInput(tpe: Type)(implicit definitions: Definitions): TermName = {
    val inputType = tpe match {
      case OptionType(subType)                   ⇒ q"sangria.schema.OptionType(${getOutput(subType)})"
      case SeqType(subType)                      ⇒ q"sangria.schema.ListType(${getOutput(subType)})" // FIXME use Iterable instead of Seq
      case dateType if dateType <:< typeOf[Date] ⇒ q"org.thp.scalligraph.graphql.DateType"
      case ImplicitInputType(value)              ⇒ value
      case EnumType(values @ _*) ⇒
        val enumValues = values.map { case (name, value) ⇒ q"sangria.schema.EnumValue[$tpe]($name,$value)" }
        val tpeName    = q"${tpe.typeSymbol.name.decodedName.toString}"
        q"sangria.schema.EnumType($tpeName, List(..$enumValues))"

    }
    definitions.add(inputType)
  }
}
