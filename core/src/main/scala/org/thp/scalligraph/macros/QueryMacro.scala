package org.thp.scalligraph.macros
import org.thp.scalligraph.query._
import org.thp.scalligraph.{MacroLogger, MacroUtil}
import scala.reflect.macros.blackbox

class QueryMacro(val c: blackbox.Context) extends FieldsParserUtil with MacroLogger with MacroUtil {

  import c.universe._

  def extractBuilder[Q: WeakTypeTag](queryExecutor: Tree, builderType: Type, queryType: Type): Tree = {
    val qType = weakTypeOf[Q]
    val (defs, parsers, acceptedInput) = qType.members.foldLeft((List.empty[Tree], List.empty[Tree], List.empty[String])) {
      case ((_defs, _parsers, _acceptedInput), s) if s.isMethod && s.asMethod.returnType <:< builderType ⇒
        val name       = s.name.decodedName.toString.trim
        val returnType = s.asMethod.returnType
        val paramType  = getTypeArgs(returnType, builderType).head
        info(s"Found $s is $builderType (from $paramType)")
        if (paramType <:< typeOf[Unit])
          (
            _defs,
            cq"(_, FNamedObj($name, _)) ⇒ Good($queryExecutor.${TermName(s.name.toString)}.apply(()))" :: _parsers,
            s"'_name: $name" :: _acceptedInput)
        else
          getOrBuildParser(paramType.typeSymbol, paramType)
            .map { parser ⇒
              val defName = TermName(c.freshName())
              (
                q"val $defName = $parser" :: _defs,
                cq"(_, FNamedObj($name, field)) ⇒ $defName.map($name)(p ⇒ $queryExecutor.${TermName(s.name.toString)}(p))(field)" :: _parsers,
                s"'_name: $name, $paramType" :: _acceptedInput)
            }
            .getOrElse {
              warn(s"No parser found for $paramType")
              (_defs, _parsers, _acceptedInput)
            }
      case (x, _) ⇒ x
    }
    q"""
      import org.thp.scalligraph.controllers.FieldsParser
      import org.thp.scalligraph.query.FNamedObj

      ..$defs
      FieldsParser[$queryType](${builderType.typeSymbol.name.decodedName.toString.trim}, $acceptedInput) {
        case ..$parsers
      }
     """
  }

  def extractQuery[Q: WeakTypeTag](queryExecutor: Tree, queryType: Type): Tree = {
    val qType = weakTypeOf[Q]
    val (queryCases, acceptedInputs) = qType.members.foldLeft((List.empty[Tree], List.empty[String])) {
      case ((_queryCases, _acceptedInputs), s) if s.isMethod && s.asMethod.returnType <:< queryType ⇒
        val name       = s.name.decodedName.toString.trim
        info(s"Found $s is $queryType")
        (cq"(_, FNamedObj($name, field)) ⇒  Good($queryExecutor.${TermName(s.name.toString)})" :: _queryCases,
          s"'name: $name" :: _acceptedInputs)
      case (x, _) ⇒ x
    }
    q"""
      import org.thp.scalligraph.query.FNamedObj

      FieldsParser[$queryType](${queryType.typeSymbol.name.decodedName.toString.trim}, Seq(..$acceptedInputs)) {
        case ..$queryCases
      }
     """
  }

  def queryBuilderParser[Q: WeakTypeTag](queryExecutor: Tree): Tree = {
    initLogger(weakTypeOf[Q].typeSymbol)
    ret(
      "query parser",
      q"""
      import org.thp.scalligraph.controllers.{FieldsParser, FSeq}
      import org.thp.scalligraph.InvalidFormatAttributeError
      import org.thp.scalligraph.query.InitQuery
      import org.scalactic.{Bad, Good, One}

      val initQueryParser = ${extractBuilder[Q](queryExecutor, typeOf[InitQueryBuilder[_, _]], typeOf[InitQuery[_]])}
        .orElse(${extractQuery[Q](queryExecutor,  typeOf[InitQuery[_]])})
      val queryParser = ${extractBuilder[Q](queryExecutor, typeOf[QueryBuilder[_, _]], typeOf[QueryComposable[_]])}
        .orElse(${extractQuery[Q](queryExecutor, typeOf[Query[_, _]])})
      FieldsParser("", initQueryParser.acceptedInput ++ queryParser.acceptedInput) {
        case (_, FSeq(head :: tail)) ⇒
          tail.foldLeft(initQueryParser(head)) {
            case (Good(query), field) ⇒
              queryParser(field).map(_.compose(query))
            case (bad, _) ⇒ bad
        }
      }
     """
    )
  }
}
