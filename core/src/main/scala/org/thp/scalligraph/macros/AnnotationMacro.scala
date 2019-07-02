package org.thp.scalligraph.macros

import scala.reflect.macros.whitebox
import scala.util.{Try => UTry}

import org.thp.scalligraph.{MacroLogger, MacroUtil}

class AnnotationMacro(val c: whitebox.Context) extends MacroUtil with MappingMacroHelper with MacroLogger {

  import c.universe._

  def buildVertexModel(annottees: Tree*): Tree =
    annottees.toList match {
      case (modelClass @ ClassDef(classMods, className, Nil, _)) :: tail if classMods.hasFlag(Flag.CASE) =>
        val modelDef = Seq(
          q"val model = org.thp.scalligraph.models.Model.vertex[$className]"
        )

        val modelModule = tail match {
          case ModuleDef(moduleMods, moduleName, moduleTemplate) :: Nil =>
            val parents = tq"org.thp.scalligraph.models.HasVertexModel[$className]" :: moduleTemplate.parents.filterNot {
              case Select(_, TypeName("AnyRef")) => true
              case _                             => false
            }

            ModuleDef(
              moduleMods,
              moduleName,
              Template(parents = parents, self = moduleTemplate.self, body = moduleTemplate.body ++ modelDef)
            )
          case Nil =>
            val moduleName = className.toTermName
            q"object $moduleName extends org.thp.scalligraph.models.HasVertexModel[$className] { ..$modelDef }"
        }

        Block(modelClass :: modelModule :: Nil, Literal(Constant(())))
    }

  def buildEdgeModel(annottees: Tree*): Tree = {
    val (fromType, toType) = c.macroApplication match {
      case q"new $_[$from, $to].macroTransform(..$_)" =>
        UTry(c.typecheck(q"0.asInstanceOf[$from]").tpe -> c.typecheck(q"0.asInstanceOf[$to]").tpe)
          .getOrElse(c.abort(c.enclosingPosition, "FIXME"))
      case _ =>
        c.abort(c.enclosingPosition, s"macroApplication = ${showRaw(c.macroApplication)}")
    }
    annottees.toList match {
      case (modelClass @ ClassDef(classMods, className, Nil, _)) :: tail if classMods.hasFlag(Flag.CASE) =>
        val modelDef = Seq(
          q"val model = org.thp.scalligraph.models.Model.edge[$className, $fromType, $toType]"
        )
        val modelModule = tail match {
          case ModuleDef(moduleMods, moduleName, moduleTemplate) :: Nil =>
            val parents = tq"org.thp.scalligraph.models.HasEdgeModel[$className, $fromType, $toType]" :: moduleTemplate.parents.filterNot {
              case Select(_, TypeName("AnyRef")) => true
              case _                             => false
            }
            ModuleDef(
              moduleMods,
              moduleName,
              Template(
                parents = parents,
                self = moduleTemplate.self,
                body = moduleTemplate.body ++ modelDef
              )
            )
          case Nil =>
            val moduleName = className.toTermName
            q"object $moduleName extends org.thp.scalligraph.models.HasEdgeModel[$className, $fromType, $toType] { ..$modelDef }"
        }

        Block(modelClass :: modelModule :: Nil, Literal(Constant(())))
    }
  }

  def outputImpl(annottees: Tree*): Tree =
    annottees.toList match {
      case ClassDef(classMods, className, Nil, template) :: tail if classMods.hasFlag(Flag.CASE) =>
        val writes = TermName(c.freshName("writes"))

        val writesDef =
          q"private val $writes = org.thp.scalligraph.models.JsonWrites.apply[$className]"
        val toJsonDef =
          q"def toJson: play.api.libs.json.JsObject = $writes.writes(this).as[play.api.libs.json.JsObject]"
        val toEntityJsonDef =
          q"""
            def toJson(e: org.thp.scalligraph.models.Entity): play.api.libs.json.JsObject = toJson +
              ("_id"        -> play.api.libs.json.JsString(e._id.asInstanceOf[String])) +
              ("_createdAt" -> play.api.libs.json.JsNumber(e._createdAt.getTime())) +
              ("_createdBy" -> play.api.libs.json.JsString(e._createdBy)) +
              ("_updatedAt" -> e._updatedAt.fold[play.api.libs.json.JsValue](play.api.libs.json.JsNull)(d ⇒ play.api.libs.json.JsNumber(d.getTime()))) +
              ("_updatedBy" -> e._updatedBy.fold[play.api.libs.json.JsValue](play.api.libs.json.JsNull)(play.api.libs.json.JsString.apply)) +
              ("_type"      -> play.api.libs.json.JsString(${className.toTermName.toString}))
          """
        val classDef =
          ClassDef(classMods, className, Nil, Template(template.parents, template.self, template.body :+ writesDef :+ toJsonDef :+ toEntityJsonDef))
        Block(classDef :: tail, Literal(Constant(())))
    }

  def entitySteps(annottees: Tree*): Tree = {
    val entityClass: Tree = c.prefix.tree match {
      case q"new $_[$typ]" => typ.asInstanceOf[Tree]
      case _ =>
        fatal("Transform annotation is malformed")
    }
    val entityClassType: Type = UTry(c.typecheck(q"0.asInstanceOf[$entityClass]").tpe)
      .getOrElse(fatal(s"Type of target class ($entityClass) can't be identified. It must not be in the same scope of the annoted class."))
    initLogger(entityClassType.typeSymbol)

    annottees.toList match {
      case ClassDef(classMods, className, tparams, classTemplate) :: _ =>
        val entityFields = entityClassType match {
          case CaseClassType(fields @ _*) =>
            fields.map { field =>
              val mapping     = getMapping(field, field.typeSignature)
              val mappingName = TermName(c.freshName(s"${field.name}Mapping"))
              q"""
                def ${TermName(field.name.toString)} = {
                  val $mappingName = $mapping
                  org.thp.scalligraph.models.ScalarSteps(raw.properties(${field
                .name
                .decodedName
                .toString
                .trim}).map(_.value.asInstanceOf[$mappingName.GraphType]))
                }
              """
            }
        }
        ClassDef(classMods, className, tparams, Template(classTemplate.parents, classTemplate.self, classTemplate.body ++ entityFields))
    }
  }
}
