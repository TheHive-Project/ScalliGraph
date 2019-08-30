package org.thp.scalligraph.macros

import scala.reflect.macros.whitebox
import scala.util.{Try => UTry}

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
