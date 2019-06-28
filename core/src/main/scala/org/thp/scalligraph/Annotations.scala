package org.thp.scalligraph

import scala.annotation.{compileTimeOnly, StaticAnnotation}
import scala.language.experimental.{macros => enableMacros}

import play.api.libs.json.Writes

import org.thp.scalligraph.macros.AnnotationMacro

@compileTimeOnly("enable macro paradise to expand macro annotations")
class VertexEntity extends StaticAnnotation {

  def macroTransform(annottees: Any*): Any =
    macro AnnotationMacro.buildVertexModel
}

@compileTimeOnly("enable macro paradise to expand macro annotations")
class EdgeEntity[FROM <: Product, TO <: Product] extends StaticAnnotation {

  def macroTransform(annottees: Any*): Any =
    macro AnnotationMacro.buildEdgeModel
}

@compileTimeOnly("enable macro paradise to expand macro annotations")
class JsonOutput extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro AnnotationMacro.outputImpl
}

class FromEntity

@compileTimeOnly("enable macro paradise to expand macro annotations")
class EntitySteps[E <: Product] extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro AnnotationMacro.entitySteps
}

//class PrivateField extends StaticAnnotation

class WithOutput[A](writes: Writes[A]) extends StaticAnnotation
