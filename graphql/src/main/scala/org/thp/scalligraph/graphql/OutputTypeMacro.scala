package org.thp.scalligraph.graphql

import java.util.Date

import gremlin.scala.GremlinScala
import org.thp.scalligraph.models.{EntityFilter, ScalliSteps}
import sangria.marshalling.FromInput
import sangria.schema.OutputType
import sangria.util.tag.@@

import scala.reflect.macros.blackbox

trait OutputTypeMacro {
  _: SchemaMacro ⇒
  val c: blackbox.Context

  import c.universe._

  object ImplicitOutputType {
    def unapply(tpe: Type): Option[Tree] = {
      val oututType   = appliedType(typeOf[sangria.schema.OutputType[_]].typeConstructor, tpe)
      val outputValue = c.inferImplicitValue(oututType, silent = true)
      if (outputValue.tpe =:= NoType) {
        debug(s"Implicit $oututType not found")
        None
      } else Some(outputValue)
    }
  }

  def buildArgument(s: Symbol)(implicit definitions: Definitions): TermName = {
    val argumentName = s.name.decodedName.toString.trim
    val argumentType = getInput(s.typeSignature)
    definitions.add(q"""
         sangria.schema.Argument($argumentName, $argumentType)
       """)
  }

  def getQueryFields(tpe: Type)(implicit definitions: Definitions): Seq[Tree] =
    definitions.queries.filter(tpe <:< _._2).map {
      case (method, _, outputType) ⇒
        val fieldName        = method.name.decodedName.toString.trim
        val outputObjectType = getOutput(outputType)
        val arguments        = method.paramLists.headOption.getOrElse(Nil).map(buildArgument)
        val argumentValues   = q"ctx.value" +: arguments.map(arg ⇒ q"ctx.arg($arg)")

        q"""
          sangria.schema.Field(
            $fieldName,
            $outputObjectType,
            arguments = List(..$arguments),
            resolve = ctx => ${definitions.executor}.$method(..$argumentValues))
        """
    }

  def buildFilterField(domainType: Type, graphType: Type, outputType: Type)(implicit definitions: Definitions): Tree =
    Some(domainType)
      .flatMap {
        case RefinedType((cc @ CaseClassType(s @ _*)) :: _, _) ⇒ Some(cc → s)
        case cc @ CaseClassType(s @ _*)                        ⇒ Some(cc → s)
        case _                                                 ⇒ None
      }
      .map {
        case (domainClass, symbols) ⇒
          val name = domainClass.typeSymbol.name.decodedName.toString.trim + "Filter"
          val filterInputObjectType = definitions.getOrAddFilter(domainType) {
            val fields = definitions.add(
              symbols
                .map { symbol ⇒
                  val sTpe  = symbol.typeSignature
                  val sName = symbol.name.decodedName.toString.trim
                  if (sTpe <:< typeOf[String])
                    q"org.thp.scalligraph.graphql.Filter.string[$graphType]($sName)"
                  else if (sTpe <:< typeOf[Int])
                    q"org.thp.scalligraph.graphql.Filter.int[$graphType]($sName)"
                  else {
                    warn(s"Field $sName in $domainType has an unrecognized type for filter ($sTpe)")
                    q"Nil"
                  }
                }
                .reduceOption((a, b) ⇒ q"$a ::: $b")
                .getOrElse(q"Nil"))

            val entityFilterType = appliedType(typeOf[EntityFilter[_]].typeConstructor, graphType)
            val inputTaggedType  = appliedType(typeOf[@@[_, _]].typeConstructor, entityFilterType, typeOf[FromInput.InputObjectResult])
            val fromInputType    = appliedType(typeOf[FromInput[_]].typeConstructor, inputTaggedType)
            definitions.addInput(fromInputType, q"org.thp.scalligraph.graphql.Filter.fromInput[$graphType]($fields)")
            q"org.thp.scalligraph.graphql.Filter.inputObjectType[$graphType]($name, $fields)"
          }
          val arg = definitions.add(q"""sangria.schema.Argument("filter", $filterInputObjectType)""")
          q"""
       sangria.schema.fields[org.thp.scalligraph.query.AuthGraph, $outputType](sangria.schema.Field(
         "filter",
         ${getOutput(outputType)},
         arguments = List($arg),
         resolve = ctx => ctx.value.filter(ctx.arg($arg))
       ))
     """
      }
      .getOrElse(q"Nil")

  def buildStepObject(tpe: Type)(implicit definitions: Definitions): Tree = {
    val st :: graphType :: labels :: Nil = getTypeArgs(tpe, typeOf[ScalliSteps[_, _, _]])
    val subType                          = if (st.typeSymbol.asType.isAbstract) tpe.typeArgs.head else st
    debug(s"step sub type of $tpe is $subType / $graphType / $labels")
    val output        = getOutput(tpe)
    val subOutputType = getOutput(subType)
    val fields        = q"org.thp.scalligraph.graphql.Schema.scalliStepsFields[$tpe, $subType]($output, $subOutputType)"
    val objectClass = subType match {
      case RefinedType(t :: _, _) ⇒ t
      case _                      ⇒ subType
    }
    val classFields = objectClass match {
      case CaseClassType(symbols @ _*) ⇒
        val symbolNames = symbols.map(_.name.toString.trim)
        getFields(tpe.members.filter(m ⇒ symbolNames.contains(m.name.toString.trim)).toSeq)
      case _ ⇒ Nil
    }
    val objectName  = objectClass.typeSymbol.name.decodedName.toString.trim + "Traversal"
    val filterField = buildFilterField(subType, graphType, tpe)
    q"""
      sangria.schema.ObjectType(
        $objectName,
        () => sangria.schema.fields[org.thp.scalligraph.query.AuthGraph, $tpe](..${getQueryFields(tpe) ++ classFields}) ::: $filterField ::: $fields)
    """
  }

  def getFields(symbols: Seq[Symbol])(implicit definitions: Definitions): Seq[Tree] =
    symbols
      .map { s ⇒
        val fieldName  = s.name.decodedName.toString.trim
        val tpe        = if (s.isMethod) s.asMethod.returnType else s.typeSignature
        val outputType = getOutput(tpe)
        q"""
          sangria.schema.Field(
            $fieldName,
            $outputType,
            resolve = _.value.${TermName(fieldName)})
        """
      }

  def buildTraversalObject(tpe: Type)(implicit definitions: Definitions): Tree = {
    val subType = getTypeArgs(tpe, typeOf[GremlinScala[_]]).head
    println(s"traversal sub type of $tpe is $subType")
    val subOutputType = getOutput(subType)
    val fields        = q"org.thp.scalligraph.graphql.Schema.gremlinScalaFields[$tpe, $subType](${getOutput(tpe)}, $subOutputType)"
    val objectClass = subType match {
      case RefinedType(t :: _, _) ⇒ t
      case _                      ⇒ subType
    }
    val objectName = objectClass.typeSymbol.name.decodedName.toString.trim + "Traversal"
    q"""
      sangria.schema.ObjectType(
        $objectName,
        () => sangria.schema.fields[org.thp.scalligraph.query.AuthGraph, $tpe](..${getQueryFields(tpe)}) ::: $fields)
    """
  }

  def buildClassObject(objectName: String, tpe: Type, symbols: Seq[Symbol])(implicit definitions: Definitions): Tree =
    q"""
      sangria.schema.ObjectType(
        $objectName,
        () => sangria.schema.fields[org.thp.scalligraph.query.AuthGraph, $tpe](..${getQueryFields(tpe) ++ getFields(symbols)}))
    """

  def buildEnumType(tpe: Type, values: Seq[(Tree, Tree)]): Tree = {
    val enumValues = values.map { case (name, value) ⇒ q"sangria.schema.EnumValue[$tpe](name = $name, value = $value)" }
    val tpeName    = q"${tpe.typeSymbol.name.decodedName.toString}"
    q"sangria.schema.EnumType(name = $tpeName, values = List(..$enumValues))"
  }

  def getOutput(tpe: Type)(implicit definitions: Definitions): TermName = {
    val outputType = appliedType(typeOf[OutputType[_]], tpe)
    debug(s"getOutput($tpe)")
    definitions.getOrAdd(outputType) {
      tpe match {
        case OptionType(subType)                                     ⇒ q"sangria.schema.OptionType(${getOutput(subType)})"
        case SeqType(subType)                                        ⇒ q"sangria.schema.ListType(${getOutput(subType)})" // FIXME use Iterable instead of Seq
        case ImplicitOutputType(value)                               ⇒ value
        case EnumType(values @ _*)                                   ⇒ buildEnumType(tpe, values)
        case _ if tpe <:< typeOf[Date]                               ⇒ q"org.thp.scalligraph.graphql.DateType"
        case _ if tpe <:< typeOf[ScalliSteps[_, _, _]]               ⇒ buildStepObject(tpe)
        case _ if tpe <:< typeOf[GremlinScala[_]]                    ⇒ buildTraversalObject(tpe)
        case RefinedType((cc @ CaseClassType(symbols @ _*)) :: _, _) ⇒ buildClassObject(cc.typeSymbol.name.decodedName.toString.trim, tpe, symbols)
        case CaseClassType(symbols @ _*)                             ⇒ buildClassObject(tpe.typeSymbol.name.decodedName.toString.trim, tpe, symbols)

      }
    }
  }
}
